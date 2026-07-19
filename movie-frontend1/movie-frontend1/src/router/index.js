import { createRouter, createWebHistory } from 'vue-router';
import MovieSearch from '../views/MovieSearch.vue'; // 引入页面

const routes = [
    { path: '/', name: 'movieSearch', component: MovieSearch } // 根路径指向查询页
];

const router = createRouter({ history: createWebHistory(), routes });
export default router;