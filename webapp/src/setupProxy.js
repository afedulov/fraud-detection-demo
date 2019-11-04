const proxy = require("http-proxy-middleware");

module.exports = function(app) {
  app.use(proxy("/api", { target: "http://localhost:5656" }));
  app.use(proxy("/ws", { target: "ws://localhost:5656", ws: true }));
};
