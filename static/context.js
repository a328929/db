(() => {
  "use strict";

  const display = window.TgHarvestDisplay;

  const root = document.getElementById("app");
  const config = {
      chatId: root ? root.dataset.chatId : "",
      msgId: new URLSearchParams(window.location.search).get("msg_id")
  };
  if (!config.chatId || !config.msgId) {
      document.getElementById("statusLine").textContent = "缺少必要的群组或消息参数。";
      return;
  }

  const state = {
      chatId: parseInt(config.chatId, 10),
      targetMsgId: parseInt(config.msgId, 10),
      oldestMsgId: null,
      newestMsgId: null,
      isLoading: false
  };

  const els = {
      messageList: document.getElementById("messageList"),
      loadBeforeBtn: document.getElementById("loadBeforeBtn"),
      loadAfterBtn: document.getElementById("loadAfterBtn"),
      statusLine: document.getElementById("statusLine"),
      pageTitle: document.getElementById("pageTitle")
  };

  function _appendMessageTitleContent(titleEl, item) {
      display.appendBadgesAndText(titleEl, item, item.content || item.title);
  }

  function _buildMessageElement(item) {
      const li = document.createElement("li");
      li.className = "result-item context-msg-item";
      li.id = "msg-" + item.message_id;

      if (item.message_id === state.targetMsgId) {
          li.classList.add("is-target");
      }
      if (item.is_promo) li.classList.add("is-promo");

      const h3 = document.createElement("h3");
      h3.className = "result-title";
      _appendMessageTitleContent(h3, item);
      li.appendChild(h3);

      const mediaParts = [];
      if (item.file_size != null) {
          mediaParts.push(display.formatFileSize(item.file_size));
      }
      if (item.duration_sec != null) {
          mediaParts.push(display.formatDuration(item.duration_sec));
      }

      const metaFooter = document.createElement("div");
      metaFooter.className = "result-meta";

      if (mediaParts.length > 0) {
          const mediaSpan = document.createElement("span");
          mediaSpan.textContent = `媒体: ${mediaParts.join(" | ")}`;
          metaFooter.appendChild(mediaSpan);
      }

      if (item.link) {
          const linkSpan = document.createElement("span");
          if (mediaParts.length > 0) linkSpan.className = "context-meta-link";
          const a = document.createElement("a");
          a.href = item.link;
          a.target = "_blank";
          a.rel = "noopener noreferrer";
          a.className = "result-link";
          a.textContent = "跳转至原生客户端";
          a.setAttribute("aria-label", "跳转至 Telegram 原生客户端查看该消息");
          linkSpan.appendChild(a);
          metaFooter.appendChild(linkSpan);
      }

      li.appendChild(metaFooter);
      return li;
  }

  function _focusTargetItem(el) {
      if (!el) return;
      const titleEl = el.querySelector("h3.result-title") || el;
      titleEl.setAttribute("tabindex", "-1");
      titleEl.focus();
      titleEl.addEventListener("blur", () => {
          titleEl.removeAttribute("tabindex");
      }, { once: true });
  }

  async function loadData(direction) {
      if (state.isLoading) return;
      state.isLoading = true;
      els.statusLine.textContent = "加载中...";

      let anchorId = state.targetMsgId;
      if (direction === "before" && state.oldestMsgId !== null) {
          anchorId = state.oldestMsgId;
      } else if (direction === "after" && state.newestMsgId !== null) {
          anchorId = state.newestMsgId;
      }

      try {
          const resp = await fetch(`/api/chat/${state.chatId}/context?msg_id=${anchorId}&direction=${direction}`);
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const data = await resp.json();
          if (!data.ok) throw new Error(data.error || "加载失败");

          const items = data.items || [];
          if (items.length === 0) {
              els.statusLine.textContent = "没有更多消息了。";
              state.isLoading = false;
              return;
          }

          els.statusLine.textContent = "";

          if (items[0] && items[0].chat_title) {
              els.pageTitle.textContent = `${items[0].chat_title} - 上下文阅读`;
              document.title = `${items[0].chat_title} - 上下文`;
          }

          if (direction === "around") {
              els.messageList.textContent = "";
              items.forEach(item => els.messageList.appendChild(_buildMessageElement(item)));

              const targetEl = document.getElementById("msg-" + state.targetMsgId);
              if (targetEl) {
                  _focusTargetItem(targetEl);
                  targetEl.scrollIntoView({ block: "center", behavior: "auto" });
              }
              state.oldestMsgId = items[0].message_id;
              state.newestMsgId = items[items.length - 1].message_id;

          } else if (direction === "before") {
              const firstChild = els.messageList.firstElementChild;
              const fragment = document.createDocumentFragment();
              items.forEach(item => fragment.appendChild(_buildMessageElement(item)));
              els.messageList.insertBefore(fragment, firstChild);

              if (firstChild) {
                  _focusTargetItem(firstChild);
                  firstChild.scrollIntoView({ block: "start", behavior: "auto" });
              }
              state.oldestMsgId = items[0].message_id;

          } else if (direction === "after") {
              const lastChild = els.messageList.lastElementChild;
              const fragment = document.createDocumentFragment();
              items.forEach(item => fragment.appendChild(_buildMessageElement(item)));
              els.messageList.appendChild(fragment);

              if (lastChild && lastChild.nextElementSibling) {
                  _focusTargetItem(lastChild.nextElementSibling);
              }
              state.newestMsgId = items[items.length - 1].message_id;
          }

      } catch (err) {
          els.statusLine.textContent = `加载失败: ${err.message}`;
      } finally {
          state.isLoading = false;
      }
  }

  els.loadBeforeBtn.addEventListener("click", () => loadData("before"));
  els.loadAfterBtn.addEventListener("click", () => loadData("after"));

  loadData("around");

})();
