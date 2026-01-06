package com.study.grpcstudytodoservice.service;

import com.example.notification.grpc.NotificationServiceGrpc;
import com.example.notification.grpc.SendNotificationRequest;
import com.study.grpcstudytodoservice.*;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class TodoServiceImpl extends TodoServiceGrpc.TodoServiceImplBase {

    private final Map<String, Todo> todoStore = new ConcurrentHashMap<>();
    private final List<StreamObserver<TodoUpdate>> subscribers = new CopyOnWriteArrayList<>();

    @GrpcClient("notification-service")
    private NotificationServiceGrpc.NotificationServiceBlockingStub notificationStub;

    @Override
    public void createTodo(CreateTodoRequest request, StreamObserver<TodoResponse> responseObserver) {
        log.info("Creating todo: {}", request.getTitle());

        String id = UUID.randomUUID().toString();
        long now = Instant.now().toEpochMilli();

        Todo todo = Todo.newBuilder()
                .setId(id)
                .setTitle(request.getTitle())
                .setDescription(request.getDescription())
                .setCompleted(false)
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .setUserId(request.getUserId())
                .build();

        todoStore.put(id, todo);

        // 알림 전송
        sendNotification(request.getUserId(), "Todo Created",
                "New todo created: " + request.getTitle(), "SUCCESS");

        // 구독자들에게 업데이트 브로드캐스트
        broadcastUpdate("CREATED", todo);

        TodoResponse response = TodoResponse.newBuilder()
                .setTodo(todo)
                .setMessage("Todo created successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getTodo(GetTodoRequest request, StreamObserver<TodoResponse> responseObserver) {
        log.info("Getting todo: {}", request.getId());

        Todo todo = todoStore.get(request.getId());

        if (todo == null) {
            responseObserver.onError(
                    io.grpc.Status.NOT_FOUND
                            .withDescription("Todo not found")
                            .asRuntimeException()
            );
            return;
        }

        TodoResponse response = TodoResponse.newBuilder()
                .setTodo(todo)
                .setMessage("Todo retrieved successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateTodo(UpdateTodoRequest request, StreamObserver<TodoResponse> responseObserver) {
        log.info("Updating todo: {}", request.getId());

        Todo existingTodo = todoStore.get(request.getId());

        if (existingTodo == null) {
            responseObserver.onError(
                    io.grpc.Status.NOT_FOUND
                            .withDescription("Todo not found")
                            .asRuntimeException()
            );
            return;
        }

        Todo updatedTodo = existingTodo.toBuilder()
                .setTitle(request.getTitle())
                .setDescription(request.getDescription())
                .setCompleted(request.getCompleted())
                .setUpdatedAt(Instant.now().toEpochMilli())
                .build();

        todoStore.put(request.getId(), updatedTodo);

        // 알림 전송
        sendNotification(existingTodo.getUserId(), "Todo Updated",
                "Todo updated: " + request.getTitle(), "INFO");

        // 구독자들에게 업데이트 브로드캐스트
        broadcastUpdate("UPDATED", updatedTodo);

        TodoResponse response = TodoResponse.newBuilder()
                .setTodo(updatedTodo)
                .setMessage("Todo updated successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTodo(DeleteTodoRequest request, StreamObserver<DeleteTodoResponse> responseObserver) {
        log.info("Deleting todo: {}", request.getId());

        Todo todo = todoStore.remove(request.getId());

        if (todo == null) {
            responseObserver.onError(
                    io.grpc.Status.NOT_FOUND
                            .withDescription("Todo not found")
                            .asRuntimeException()
            );
            return;
        }

        // 알림 전송
        sendNotification(todo.getUserId(), "Todo Deleted",
                "Todo deleted: " + todo.getTitle(), "WARNING");

        // 구독자들에게 업데이트 브로드캐스트
        broadcastUpdate("DELETED", todo);

        DeleteTodoResponse response = DeleteTodoResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Todo deleted successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void listTodos(ListTodosRequest request, StreamObserver<TodoListResponse> responseObserver) {
        log.info("Listing todos for user: {}", request.getUserId());

        List<Todo> userTodos = todoStore.values().stream()
                .filter(todo -> todo.getUserId().equals(request.getUserId()))
                .sorted(Comparator.comparing(Todo::getCreatedAt).reversed())
                .toList();

        TodoListResponse response = TodoListResponse.newBuilder()
                .addAllTodos(userTodos)
                .setTotal(userTodos.size())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void streamTodoUpdates(StreamRequest request, StreamObserver<TodoUpdate> responseObserver) {
        log.info("New subscriber for todo updates: {}", request.getUserId());

        subscribers.add(responseObserver);

        // 연결이 끊어질 때 구독자 제거
        // 실제로는 Context를 사용하여 취소를 감지해야 합니다
    }

    private void broadcastUpdate(String eventType, Todo todo) {
        TodoUpdate update = TodoUpdate.newBuilder()
                .setEventType(eventType)
                .setTodo(todo)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        subscribers.forEach(observer -> {
            try {
                observer.onNext(update);
            } catch (Exception e) {
                log.error("Error broadcasting to subscriber", e);
                subscribers.remove(observer);
            }
        });
    }

    private void sendNotification(String userId, String title, String message, String type) {
        try {
            SendNotificationRequest notificationRequest = SendNotificationRequest.newBuilder()
                    .setUserId(userId)
                    .setTitle(title)
                    .setMessage(message)
                    .setType(type)
                    .build();

            notificationStub.sendNotification(notificationRequest);
        } catch (Exception e) {
            log.error("Failed to send notification", e);
        }
    }

}
