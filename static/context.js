(() => {
  "use strict";

  const display = window.TgHarvestDisplay;

  const root = document.getElementById("app");
  const config = {
      chatId: root ? root.dataset.chatId : "",
      msgId: new URLSearchParams(window.location.search).get("msg_id")
  };
  const parsedChatId = Number(config.chatId);
  const parsedMsgId = Number(config.msgId);
  if (
      !/^-?[1-9]\d*$/.test(String(config.chatId || ""))
      || !/^[1-9]\d*$/.test(String(config.msgId || ""))
      || !Number.isSafeInteger(parsedChatId)
      || !Number.isSafeInteger(parsedMsgId)
  ) {
      document.getElementById("statusLine").textContent = "缺少或无法识别群组、消息参数。";
      document.getElementById("loadBeforeBtn").disabled = true;
      document.getElementById("loadAfterBtn").disabled = true;
      return;
  }

  const state = {
      chatId: parsedChatId,
      targetMsgId: parsedMsgId,
      oldestMsgId: null,
      newestMsgId: null,
      isLoading: false,
      hasMoreBefore: true,
      hasMoreAfter: true
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

  function updateLoadButtonState() {
      els.loadBeforeBtn.disabled = state.isLoading || !state.hasMoreBefore;
      els.loadAfterBtn.disabled = state.isLoading || !state.hasMoreAfter;
      els.loadBeforeBtn.setAttribute("aria-busy", state.isLoading ? "true" : "false");
      els.loadAfterBtn.setAttribute("aria-busy", state.isLoading ? "true" : "false");
  }

  function markDirectionExhausted(direction) {
      if (direction === "before") {
          state.hasMoreBefore = false;
          els.statusLine.textContent = "没有更早的消息了。";
          return;
      }
      if (direction === "after") {
          state.hasMoreAfter = false;
          els.statusLine.textContent = "没有更新的消息了。";
          return;
      }
      state.hasMoreBefore = false;
      state.hasMoreAfter = false;
      els.statusLine.textContent = "未找到目标消息上下文。";
  }

  function updateAroundBoundaryState(items) {
      const targetIndex = items.findIndex(item => item.message_id === state.targetMsgId);
      if (targetIndex < 0) return;
      state.hasMoreBefore = targetIndex >= 50;
      state.hasMoreAfter = (items.length - targetIndex - 1) >= 50;
  }

  async function loadData(direction) {
      if (state.isLoading) return;
      state.isLoading = true;
      updateLoadButtonState();
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
              markDirectionExhausted(direction);
              state.isLoading = false;
              updateLoadButtonState();
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
              updateAroundBoundaryState(items);

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
              if (items.length < 100) {
                  state.hasMoreBefore = false;
              }

          } else if (direction === "after") {
              const lastChild = els.messageList.lastElementChild;
              const fragment = document.createDocumentFragment();
              items.forEach(item => fragment.appendChild(_buildMessageElement(item)));
              els.messageList.appendChild(fragment);

              if (lastChild && lastChild.nextElementSibling) {
                  _focusTargetItem(lastChild.nextElementSibling);
              }
              state.newestMsgId = items[items.length - 1].message_id;
              if (items.length < 100) {
                  state.hasMoreAfter = false;
              }
          }

      } catch (err) {
          els.statusLine.textContent = `加载失败: ${err.message}`;
      } finally {
          state.isLoading = false;
          updateLoadButtonState();
      }
  }

  els.loadBeforeBtn.addEventListener("click", () => loadData("before"));
  els.loadAfterBtn.addEventListener("click", () => loadData("after"));

  updateLoadButtonState();
  loadData("around");

})();
