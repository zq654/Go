### **一、Cookie 机制**

1. **基本原理**
   - 服务器通过响应头 `Set-Cookie` 将数据（如 Session ID）发送到客户端浏览器。
   - 浏览器在后续请求中自动携带该 Cookie 回服务器。
2. **超时控制**
   - `Expires`：指定具体过期时间（如 `Wed, 09 Jun 2025 10:18:14 GMT`）。
   - `Max-Age`：指定有效秒数（如 `3600` 表示 1 小时），优先于 `Expires`。
   - `Max-Age=0`：立即删除 Cookie；`Max-Age=-1`：仅当前会话有效（浏览器关闭后删除）。
3. **安全属性**
   - `HttpOnly`：禁止 JavaScript 访问，防止 XSS 攻击。
   - `Secure`：仅限 HTTPS 传输，防止中间人攻击。
   - `SameSite`：防范 CSRF 攻击（如 `SameSite=Lax` 或 `Strict`）。

### **二、Session 机制**

1. **基本原理**
   - 服务器生成唯一的 Session ID，并创建对应的服务器端会话数据（如用户信息）。
   - 通过 Cookie 将 Session ID 发送到客户端，后续请求中服务器通过 Session ID 查找用户状态。
2. **超时控制**
   - 服务器为每个 Session 设置过期时间（如 30 分钟），用户无操作则自动销毁。
   - 滑动窗口机制：每次用户访问时重置过期时间（如 `expire_at = now + 30分钟`）。
3. **优缺点**
   - **优点**：安全性高（敏感数据存服务器），存储容量无限制。
   - **缺点**：增加服务器存储压力，分布式系统需解决 Session 共享问题。

### **三、Token 机制（如 JWT）**

1. **基本原理**

   - 服务器生成签名的 Token（含用户信息），客户端存储并在请求头中携带。
   - 服务器验证 Token 签名和有效期，无需查询数据库即可完成身份验证。

2. **与 Cookie/Session 的对比**

   | **特性**   | **Cookie+Session**                  | **Token（如 JWT）**               |
   | ---------- | ----------------------------------- | --------------------------------- |
   | 存储位置   | 服务器（Session）+ 客户端（Cookie） | 客户端（如 localStorage、请求头） |
   | 跨域支持   | 需配置 CORS 和 Cookie 策略          | 天然支持跨域                      |
   | 分布式系统 | 需解决 Session 共享问题             | 无状态，无需共享                  |
   | 安全性     | 依赖 Cookie 安全属性                | 可通过加密、签名增强安全性        |

3. **Token 类型**

   - **Access Token**：短期有效（如 15 分钟～1 小时），存储在 `localStorage`。
   - **Refresh Token**：长期有效（如 7 天～30 天），存储在 `HttpOnly Cookie`，用于自动刷新 Access Token。

### **四、Token 刷新机制**

1. **流程**

   - 当 Access Token 过期时，客户端使用 Refresh Token 向服务器请求新的 Access Token。
   - 服务器验证 Refresh Token 有效性，生成新的 Access Token（和新的 Refresh Token）。

2. **Token 旋转（增强安全）**

   - 每次刷新时生成新的 Refresh Token，旧 Token 立即失效，降低 Token 泄露风险。

3. **并发刷新控制**

   - 使用单例模式确保同一时间只有一个刷新请求，避免冲突。

   
   

   **Token 旋转（Token Rotation）** 机制的核心思想。在刷新 Token 的过程中，每次都生成新的 AccessToken 和 RefreshToken，可以显著提升系统安全性。以下是详细说明：

   #### **Token 旋转的工作原理**

   1. **传统刷新机制**：
      - AccessToken 过期后，使用同一个 RefreshToken 获取新的 AccessToken，**RefreshToken 自身不变**。
      - 风险：若 RefreshToken 泄露，攻击者可长期盗用账户。
   2. **Token 旋转机制**：
      - 每次使用 RefreshToken 刷新时，**同时生成新的 AccessToken 和新的 RefreshToken**。
      - 旧的 RefreshToken 立即失效，客户端需保存新的 RefreshToken。
      - 优势：即使某个 RefreshToken 泄露，攻击者也只能使用一次，降低长期风险。

### **五、安全最佳实践**

1. **Cookie 安全**
   - 设置 `HttpOnly`、`Secure`、`SameSite` 属性。
   - 避免在 URL 或日志中暴露 Cookie。
2. **Token 安全**
   - 使用 HTTPS 防止 Token 传输被截获。
   - 合理设置 Token 有效期，配合 Refresh Token 机制。
   - 对敏感操作（如修改密码）要求二次验证。
3. **防攻击措施**
   - **XSS**：对用户输入进行过滤和转义，使用 CSP 限制脚本加载源。
   - **CSRF**：对 Refresh Token 使用 `SameSite=Strict` 或 `Lax`。
   - **Token 泄露**：实现 Token 黑名单机制，登出时失效相关 Token。

### **六、适用场景**

1. **传统单体应用**：推荐 `Session/Cookie`，实现简单。
2. **前后端分离 / 微服务**：推荐 `JWT`，无状态、跨域友好。
3. **第三方登录**：推荐 `OAuth 2.0 + OpenID Connect`，标准化程度高。

### **七、总结**

- **Cookie**：适合轻量级场景，依赖浏览器原生支持。
- **Session**：安全性高，适合对用户状态管理要求严格的场景。
- **Token**：灵活、跨域友好，适合现代 Web 应用（尤其是前后端分离架构）。

根据业务需求选择合适的方案，并严格遵循安全最佳实践，可有效保护系统和用户数据安全。 

