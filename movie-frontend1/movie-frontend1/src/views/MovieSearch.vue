<template>
  <!-- 原有页面结构保持不变，仅优化日志显示文本 -->
  <div style="width: 80%; margin: 50px auto;">
    <h2>电影查询（Kafka+Flink+HBase 实时评分版）</h2>
    <!-- 原有查询区域保持不变 -->
    <div style="margin: 20px 0;">
      <input
          v-model="movieTitle"
          placeholder="输入电影名（比如：玩具）"
          style="padding: 8px; width: 300px;"
      >
      <button
          @click="searchMovie"
          style="padding: 8px 16px; margin-left: 10px; background: #42b983; color: white; border: none; cursor: pointer;"
      >
        查电影
      </button>
    </div>
    <!-- 模拟评分区域（仅修改日志显示逻辑） -->
    <div style="margin: 20px 0; padding: 15px; background: #f0f8fb; border-radius: 8px; border: 1px solid #e8f4f8;">
      <h3>📊 模拟实时评分生成（Kafka+Flink 流处理）</h3>
      <div style="margin: 10px 0;">
        <label>目标电影ID：</label>
        <input
            v-model="targetMovieId"
            placeholder="输入查询结果中的电影ID"
            style="padding: 6px; width: 120px; margin: 0 10px;"
        >
        <button @click="startSimulate" style="background: #2196f3; color: white; border: none; padding: 6px 12px; margin-right: 10px;">开始模拟</button>
        <button @click="stopSimulate" style="background: #ff5722; color: white; border: none; padding: 6px 12px;">停止模拟</button>
      </div>
      <p style="margin: 10px 0; color: #666; font-size: 14px;" id="simulateLog">{{ simulateLog }}</p>
    </div>
    <!-- 原有结果展示区域保持不变（评分显示样式已优化） -->
    <div v-if="movieList.length > 0">
      <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background: #f5f5f5;">
          <th style="padding: 10px; text-align: left;">电影ID</th>
          <th style="padding: 10px; text-align: left;">电影名</th>
          <th style="padding: 10px; text-align: left;">类型</th>
          <th style="padding: 10px; text-align: left;">IMDB ID</th>
          <th style="padding: 10px; text-align: left;">平均评分</th>
          <th style="padding: 10px; text-align: left;">评分人数</th>
          <th style="padding: 10px; text-align: left;">热门标签</th>
        </tr>
        <tr v-for="movie in movieList" :key="movie.movieId">
          <td style="padding: 10px;">{{ movie.movieId }}</td>
          <td style="padding: 10px;">{{ movie.title }}</td>
          <td style="padding: 10px;">{{ movie.genres }}</td>
          <td style="padding: 10px;">{{ movie.imdbId }}</td>
          <td style="padding: 10px; color: #e63946; font-weight: bold;">{{ movie.avgRating.toFixed(1) }}</td>
          <td style="padding: 10px; color: #2196f3; font-weight: bold;">{{ movie.totalRatings }}</td>
          <td style="padding: 10px;">{{ movie.topTag1 }}</td>
        </tr>
      </table>
    </div>
    <div v-else-if="hasSearched" style="color: #999;">没找到匹配的电影，换个关键词试试~</div>
  </div>
</template>
<script setup>
import { ref, onUnmounted } from 'vue';
import axios from 'axios';

// 原有响应式变量保持不变
const movieTitle = ref('');
const movieList = ref([]);
const hasSearched = ref(false);
const targetMovieId = ref('');
const simulateLog = ref('未开始模拟，输入电影ID后点击"开始模拟"');
let simulateTimer = null;
let websocket = null;

// 原有查询电影方法保持不变
const searchMovie = async () => {
  if (!movieTitle.value.trim()) {
    alert('请输入电影名！');
    return;
  }
  try {
    const res = await axios.get(
        'http://localhost:8080/api/movie/search',
        { params: { title: movieTitle.value } }
    );
    movieList.value = res.data;
    hasSearched.value = true;
    initWebSocket(); // 查询成功后初始化 WebSocket
  } catch (error) {
    alert('查询失败！检查后端是否启动');
    console.error(error);
  }
};

// 原有生成随机评分方法保持不变
const getRandomRating = () => {
  return Math.round((Math.random() * 4 + 1) * 10) / 10;
};

// 原有 WebSocket 初始化方法保持不变（接收的字段新增 currentRating）
const initWebSocket = () => {
  if (websocket) return;
  websocket = new WebSocket('ws://localhost:8080/api/movie/ws');

  websocket.onopen = () => {
    console.log('WebSocket连接成功！');
    simulateLog.value = 'WebSocket已连接，可开始模拟评分（消息提交到 Kafka）';
  };

  // 仅修改 onmessage 中的日志显示（新增本次评分）
  websocket.onmessage = (event) => {
    const updateData = JSON.parse(event.data);
    const { movieId, avgRating, totalRatings, currentRating } = updateData;

    //  更新电影列表
    movieList.value = movieList.value.map(movie => {
      if (movie.movieId === movieId) {
        movie.avgRating = avgRating;
        movie.totalRatings = totalRatings;
      }
      return movie;
    });

    // 优化日志：显示本次评分+累计结果
    simulateLog.value = `[${new Date().toLocaleTimeString()}] 电影ID=${movieId} | 本次评分：${currentRating}分 | 累计：${avgRating}分（${totalRatings}人）`;
  };

  websocket.onclose = () => {
    console.log('WebSocket连接关闭');
    websocket = null;
    simulateLog.value = 'WebSocket连接关闭，实时更新已停止';
  };

  websocket.onerror = (error) => {
    console.error('WebSocket错误：', error);
    simulateLog.value = `WebSocket错误：${error.message}`;
  };
};

// 仅修改 startSimulate 中的日志显示（提示消息提交到 Kafka）
const startSimulate = () => {
  if (!targetMovieId.value.trim()) {
    simulateLog.value = '错误：请输入目标电影ID（从查询结果中复制）';
    return;
  }
  if (simulateTimer) clearInterval(simulateTimer);
  initWebSocket();

  simulateLog.value = '正在模拟评分（每2秒提交1条到 Kafka）...';
  simulateTimer = setInterval(async () => {
    const newRating = getRandomRating();
    try {
      const res = await axios.post(
          'http://localhost:8080/api/movie/receive-rating',
          { movieId: targetMovieId.value, rating: newRating },
          { headers: { 'Content-Type': 'application/json' } }
      );
      // 显示“提交到 Kafka 成功”
      simulateLog.value = `[${new Date().toLocaleTimeString()}] 提交成功：${newRating}分 → ${res.data.msg}`;
    } catch (error) {
      simulateLog.value = `[${new Date().toLocaleTimeString()}] 提交失败：${error.message}`;
      console.error(error);
    }
  }, 2000);
};

// 原有 stopSimulate 和 onUnmounted 方法保持不变
const stopSimulate = () => {
  if (simulateTimer) {
    clearInterval(simulateTimer);
    simulateTimer = null;
    simulateLog.value = '已停止模拟评分';
  }
};

onUnmounted(() => {
  stopSimulate();
  if (websocket) {
    websocket.close();
  }
});
</script>
