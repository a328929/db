(() => {
  "use strict";

  const appLink = document.getElementById("openTelegramAppLink");
  const statusLine = document.getElementById("openTelegramStatus");
  if (!appLink || !statusLine || !appLink.href) {
    return;
  }

  statusLine.textContent = "正在尝试唤起 Telegram 客户端...";

  window.setTimeout(() => {
    window.location.href = appLink.href;
  }, 120);

  window.setTimeout(() => {
    statusLine.textContent =
      "如果 Telegram 没有自动弹出，请点击“打开 Telegram App”；若当前浏览器阻止协议唤起，可使用备用网页链接。";
  }, 1800);
})();
