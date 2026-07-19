package com.movielens.websocket;

import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * WebSocket服务：前端连接后，后端主动推送评分更新
 */
@Component
@ServerEndpoint("/api/movie/ws") // WebSocket连接地址（前端需用此地址连接）
public class WebSocketServer {

    // 存储所有在线的前端连接（线程安全，支持高并发）
    private static final Set<Session> ONLINE_SESSIONS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * 前端连接成功时触发
     */
    @OnOpen
    public void onOpen(Session session) {
        ONLINE_SESSIONS.add(session);
        System.out.println("WebSocket新连接：" + session.getId());
    }

    /**
     * 前端断开连接时触发
     */
    @OnClose
    public void onClose(Session session) {
        ONLINE_SESSIONS.remove(session);
        System.out.println("WebSocket断开连接：" + session.getId());
    }

    /**
     * 接收前端消息（本功能无需前端发消息，仅后端推送，所以空实现）
     */
    @OnMessage
    public void onMessage(String message, Session session) {
        // 无需处理前端消息，仅用于接收心跳（可选）
    }

    /**
     * 连接出错时触发
     */
    @OnError
    public void onError(Session session, Throwable error) {
        ONLINE_SESSIONS.remove(session);
        System.err.println("WebSocket错误：" + session.getId() + "，错误信息：" + error.getMessage());
        error.printStackTrace();
    }

    /**
     * 核心方法：推送评分更新到所有在线前端
     * @param updateData 需推送的更新数据（movieId、avgRating、totalRatings）
     */
    public static void pushRatingUpdate(Map<String, Object> updateData) {
        // 将更新数据转为JSON字符串（前端可直接解析）
        String jsonData = JSON.toJSONString(updateData);

        // 遍历所有在线连接，推送数据
        for (Session session : ONLINE_SESSIONS) {
            if (session.isOpen()) { // 确保连接未关闭
                // 异步推送（不阻塞后端评分处理）
                session.getAsyncRemote().sendText(jsonData);
            }
        }
    }
}