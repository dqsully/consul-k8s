// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package controller contains a reusable abstraction for efficiently
// watching for changes in resources in a Kubernetes cluster.
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Controller is a generic cache.Controller implementation that watches
// Kubernetes for changes to specific set of resources and calls the configured
// callbacks as data changes.
type Controller struct {
	Log      hclog.Logger
	Resource Resource

	informer cache.SharedIndexInformer

	// registration is the handle returned by AddEventHandler and is used to
	// detect when the initial set of OnAdd events have been delivered to the
	// handler. Combined with informer.HasSynced and queue.Len, this lets
	// HasInitialEventsProcessed report when all initial-list events have been
	// fully processed by the worker loop.
	registration cache.ResourceEventHandlerRegistration

	// queue is the workqueue used to defer event processing out of the
	// informer's event-handler callbacks. Stored on the Controller so that
	// callers can observe queue length via HasInitialEventsProcessed.
	queue workqueue.RateLimitingInterface
}

// ControllerAware is an optional interface a Resource (or its Backgrounder)
// may implement to receive a reference to the outer Controller before Run
// starts. This is used by ServiceResource (pt-5) to orchestrate cross-
// controller initial-sync ordering — once both the outer Service controller
// and the inner EndpointSlice controller report HasInitialEventsProcessed,
// the Syncer's reaper can safely be unblocked without false-positive
// "service not in serviceNames" findings.
type ControllerAware interface {
	SetController(c *Controller)
}

// Event is something that occurred to the resources we're watching.
type Event struct {
	// Key is in the form of <namespace>/<name>, e.g. default/pod-abc123,
	// and corresponds to the resource modified.
	Key string
	// Obj holds the resource that was modified at the time of the event
	// occurring. If possible, the resource should be retrieved from the informer
	// cache, instead of using this field because the cache will be more up to
	// date at the time the event is processed.
	// In some cases, such as a delete event, the resource will no longer exist
	// in the cache and then it is useful to have the resource here.
	Obj interface{}
}

// Run starts the Controller and blocks until stopCh is closed.
//
// Important: Callers must ensure that Run is only called once at a time.
func (c *Controller) Run(stopCh <-chan struct{}) {
	// Properly handle any panics
	defer utilruntime.HandleCrash()

	// Create an informer so we can keep track of all service changes.
	informer := c.Resource.Informer()
	c.informer = informer

	// Create a queue for storing items to process from the informer.
	var queueOnce sync.Once
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	c.queue = queue
	shutdown := func() { queue.ShutDown() }
	defer queueOnce.Do(shutdown)

	// Add an event handler when data is received from the informer. The
	// event handlers here will block the informer so we just offload them
	// immediately into a workqueue. The handle returned by AddEventHandler
	// lets us observe via HasSynced() whether all initial-list OnAdd events
	// have been delivered yet.
	registration, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// convert the resource object into a key (in this case
			// we are just doing it in the format of 'namespace/name')
			key, err := cache.MetaNamespaceKeyFunc(obj)
			c.Log.Debug("queue", "op", "add", "key", key)
			if err == nil {
				queue.Add(Event{Key: key, Obj: obj})
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			c.Log.Debug("queue", "op", "update", "key", key)
			if err == nil {
				queue.Add(Event{Key: key, Obj: newObj})
			}
		},
		DeleteFunc: c.informerDeleteHandler(queue),
	})
	if err != nil {
		c.Log.Error("error adding informer event handlers", err)
	}
	c.registration = registration

	// Give the Resource a reference to this Controller before we start any
	// background work. Resources that implement ControllerAware use this to
	// observe HasInitialEventsProcessed across controllers (pt-5).
	if aware, ok := c.Resource.(ControllerAware); ok {
		aware.SetController(c)
	}

	// If the type is a background syncer, then we startup the background
	// process.
	if bg, ok := c.Resource.(Backgrounder); ok {
		ctx, cancelF := context.WithCancel(context.Background())

		// Run the backgrounder
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			bg.Run(ctx.Done())
		}()

		// Start a goroutine that automatically closes the context when we stop
		go func() {
			select {
			case <-stopCh:
				cancelF()

			case <-ctx.Done():
				// Cancelled outside
			}
		}()

		// When we exit, close the context so the backgrounder ends
		defer func() {
			cancelF()
			<-doneCh
		}()
	}

	// Run the informer to start requesting resources
	go func() {
		informer.Run(stopCh)

		// We have to shut down the queue here if we stop so that
		// wait.Until stops below too. We can't wait until the defer at
		// the top since wait.Until will block.
		queueOnce.Do(shutdown)
	}()

	// Initial sync
	if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("error syncing cache"))
		return
	}
	c.Log.Debug("initial cache sync complete")

	// run the runWorker method every second with a stop channel
	wait.Until(func() {
		for c.processSingle(queue, informer) {
			// Process
		}
	}, time.Second, stopCh)
}

// HasInitialEventsProcessed reports whether the controller has finished
// processing every event that was queued by the informer's initial LIST.
// It combines three signals:
//
//   - informer.HasSynced(): the local cache has been populated by the initial LIST.
//   - registration.HasSynced(): the OnAdd handler has been invoked for every
//     item discovered by the initial LIST (i.e., everything has been enqueued).
//   - queue.Len() == 0: the worker loop has finished pulling all enqueued items
//     out of the queue.
//
// Because the OnAdd handler only enqueues to the workqueue, "all initial events
// have fired" is not the same as "all initial work is done" — the worker still
// has to drain the queue. The third condition closes that gap.
//
// Note: after WATCH events start arriving, queue.Len may transiently become
// non-zero again. Callers should treat the first observation of `true` as the
// signal that the initial-list bulk processing is complete, and not flap based
// on subsequent transient values.
func (c *Controller) HasInitialEventsProcessed() bool {
	if c.informer == nil || c.registration == nil || c.queue == nil {
		return false
	}
	if !c.informer.HasSynced() {
		return false
	}
	if !c.registration.HasSynced() {
		return false
	}
	return c.queue.Len() == 0
}

// HasSynced implements cache.Controller.
func (c *Controller) HasSynced() bool {
	if c.informer == nil {
		return false
	}

	return c.informer.HasSynced()
}

// LastSyncResourceVersion implements cache.Controller.
func (c *Controller) LastSyncResourceVersion() string {
	if c.informer == nil {
		return ""
	}

	return c.informer.LastSyncResourceVersion()
}

func (c *Controller) processSingle(
	queue workqueue.RateLimitingInterface,
	informer cache.SharedIndexInformer,
) bool {
	// Fetch the next item
	rawEvent, quit := queue.Get()
	if quit {
		return false
	}
	defer queue.Done(rawEvent)

	event, ok := rawEvent.(Event)
	if !ok {
		c.Log.Warn("processSingle: dropping event with unexpected type", "event", rawEvent)
		return true
	}

	// Get the item from the informer to ensure we have the most up-to-date
	// copy.
	key := event.Key
	item, exists, err := informer.GetIndexer().GetByKey(key)

	// If we got the item successfully, call the proper method
	if err == nil {
		c.Log.Debug("processing object", "key", key, "exists", exists)
		c.Log.Trace("processing object", "object", item)
		if !exists {
			// In the case of deletes, the item is no longer in the cache so
			// we use the copy we got at the time of the event (event.Obj).
			err = c.Resource.Delete(key, event.Obj)
		} else {
			err = c.Resource.Upsert(key, item)
		}

		if err == nil {
			queue.Forget(rawEvent)
		}
	}

	if err != nil {
		if queue.NumRequeues(event) < 5 {
			c.Log.Error("failed processing item, retrying", "key", key, "error", err)
			queue.AddRateLimited(rawEvent)
		} else {
			c.Log.Error("failed processing item, no more retries", "key", key, "error", err)
			queue.Forget(rawEvent)
			utilruntime.HandleError(err)
		}
	}

	return true
}

// GetByIndex allows querying the informer's indexer to avoid extra calls to k8s
func (c *Controller) GetByIndex(indexName, indexedValue string) ([]interface{}, error) {
	if c.informer == nil {
		return nil, nil
	}

	return c.informer.GetIndexer().ByIndex(indexName, indexedValue)
}

// informerDeleteHandler returns a function that implements
// `DeleteFunc` from the `ResourceEventHandlerFuncs` interface.
// It is split out as its own method to aid in testing.
func (c *Controller) informerDeleteHandler(queue workqueue.RateLimitingInterface) func(obj interface{}) {
	return func(obj interface{}) {
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		c.Log.Debug("queue", "op", "delete", "key", key)
		if err == nil {
			// obj might be of type `cache.DeletedFinalStateUnknown`
			// in which case we need to extract the object from
			// within that struct.
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				queue.Add(Event{Key: key, Obj: d.Obj})
			} else {
				queue.Add(Event{Key: key, Obj: obj})
			}
		}
	}
}
