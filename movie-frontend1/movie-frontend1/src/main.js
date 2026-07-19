import { createApp } from 'vue'
import App from './App.vue'
import router from './router' // 引入路由配置

createApp(App)
    .use(router) // 注册路由
    .mount('#app') // 挂载到index.html的#app节点