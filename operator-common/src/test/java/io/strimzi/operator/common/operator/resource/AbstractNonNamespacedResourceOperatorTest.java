/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
@ExtendWith(VertxExtension.class)
public abstract class AbstractNonNamespacedResourceOperatorTest<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    public static final String RESOURCE_NAME = "my-resource";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    /**
     * The type of kubernetes client to be mocked
     */
    protected abstract Class<C> clientType();

    /**
     * The type of the resource being tested
     */
    protected abstract Class<? extends Resource> resourceType();

    /**
     * Get a (new) test resource
     */
    protected abstract T resource();

    /**
     * Get a modified test resource to test how are changes handled
     */
    protected abstract T modifiedResource();

    /**
     * Configure the given {@code mockClient} to return the given {@code op}
     * that's appropriate for the kind of resource being tests.
     */
    protected abstract void mocker(C mockClient, MixedOperation<T, L, R> op);

    /** Create the subclass of ResourceOperation to be tested */
    protected abstract AbstractNonNamespacedResourceOperator<C, T, L, R> createResourceOperations(
            Vertx vertx, C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractNonNamespacedResourceOperator<C, T, L, R> createResourceOperationsWithMockedReadiness(
            Vertx vertx, C mockClient)    {
        return createResourceOperations(vertx, mockClient);
    }

    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any(HasMetadata.class))).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource())
                .onComplete(context.succeeding(ar -> {
                    verify(mockResource).get();
                    verify(mockResource).patch(any(HasMetadata.class));
                    verify(mockResource, never()).create(any());
                    verify(mockResource, never()).create();
                    verify(mockResource, never()).createOrReplace(any());
                    verify(mockCms, never()).createOrReplace(any());
                    context.completeNow();
                }));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource())
                .onComplete(context.succeeding(ar -> {
                    verify(mockResource).get();
                    verify(mockResource, never()).patch(any());
                    verify(mockResource, never()).create(any());
                    verify(mockResource, never()).create();
                    verify(mockResource, never()).createOrReplace(any());
                    verify(mockCms, never()).createOrReplace(any());
                    context.completeNow();
                }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenExistenceCheckThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            context.completeNow();
        }));
    }

    @Test
    public void testSuccessfulCreation(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);
        when(mockResource.create((T) any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperationsWithMockedReadiness(
                vertx, mockClient);


        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.succeeding(rr -> {
            verify(mockResource).get();
            verify(mockResource).create(eq(resource));
            context.completeNow();
        }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);
        when(mockResource.create((T) any())).thenThrow(ex);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            context.completeNow();
        }));
    }

    @Test
    public void testDeletionWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            context.completeNow();
        }));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeletionWhenResourceExistsStillDeletes(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(true);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> watchWasClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            context.completeNow();
        }));
    }

    @Test
    public void testReconcileThrowsWhenDeletionTimesOut(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(true);
        when(mockResource.watch(any())).thenAnswer(invocation -> (Watch) () -> watchWasClosed.set(true));

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, instanceOf(TimeoutException.class));
            verify(mockResource).delete();
            assertThat("Watch was not closed", watchWasClosed.get(), is(true));
            context.completeNow();
        })));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionSuccessful(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(Boolean.TRUE);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> watchWasClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            context.completeNow();
        }));
    }

    @Test
    public void testReconcileDeletionThrowsWhenDeleteMethodThrows(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenThrow(ex);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> watchWasClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, is(ex));
            assertThat("Watch was not closed", watchWasClosed.get(), is(true));
            context.completeNow();
        })));
    }

    @Test
    public void testReconcileDeletionThrowsWhenWatchMethodThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenThrow(ex);
        when(mockResource.watch(any())).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            context.completeNow();
        }));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionThrowsWhenDeleteMethodReturnsFalse(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(Boolean.FALSE);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> watchWasClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);


        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            context.completeNow();
        }));
    }

}

