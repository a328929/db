(() => {
  "use strict";

  const state = {
    query: "",
    chat_id: "all",
    search_type: "all",
    sort_by: "time",
    order: "desc",
    page: 1,
  };

  const els = {
    queryInput: document.getElementById("queryInput"),
    searchBtn: document.getElementById("searchBtn"),
    scopeSelect: document.getElementById("scopeSelect"),
    typeSelect: document.getElementById("typeSelect"),
    sortSelect: document.getElementById("sortSelect"),
    orderSelect: document.getElementById("orderSelect"),
    statusLine: document.getElementById("statusLine"),
    results: document.getElementById("results"),
    pagination: document.getElementById("pagination"),
  };

  function setStatus(text) {
    els.statusLine.textContent = text;
  }

  function setSearching(isSearching) {
    els.searchBtn.disabled = isSearching;
    els.searchBtn.textContent = isSearching ? "搜索中..." : "搜索";
  }

  function collectFormState({ resetPage = false } = {}) {
    state.query = els.queryInput.value || "";
    state.chat_id = els.scopeSelect.value || "all";
    state.search_type = els.typeSelect.value || "all";
    state.sort_by = els.sortSelect.value || "time";
    state.order = els.orderSelect.value || "desc";
    if (resetPage) state.page = 1;
  }

  function _decideSortAvailability(typeValue, currentSortValue) {
    const disableSize = typeValue === "all" || typeValue === "text";
    return {
      disableSize,
      shouldFallback: disableSize && currentSortValue === "size",
      fallbackValue: "time",
    };
  }

  function _applySortAvailabilityUI(sizeOption, sortSelect, decision) {
    sizeOption.disabled = decision.disableSize;

    // 附加逻辑：当前是大小排序时切回时间
    if (decision.shouldFallback) {
      sortSelect.value = decision.fallbackValue;
    }
  }

  function updateSortAvailability() {
    const typeValue = els.typeSelect.value;
    const currentSortValue = els.sortSelect.value;
    const sizeOption = els.sortSelect.querySelector('option[value="size"]');

    if (!sizeOption) return;

    const decision = _decideSortAvailability(typeValue, currentSortValue);
    _applySortAvailabilityUI(sizeOption, els.sortSelect, decision);
  }

  async function _fetchMetaData() {
    const resp = await fetch("/api/meta", {
      method: "GET",
      headers: { "Accept": "application/json" },
    });
    return resp.json();
  }

  function _handleMetaSuccess(data) {
    if (!data.ok) {
      setStatus(data.error || "读取群列表失败");
      return;
    }

    // 刷新群/频道下拉框
    const currentValue = els.scopeSelect.value || "all";
    els.scopeSelect.innerHTML = "";

    const allOpt = document.createElement("option");
    allOpt.value = "all";
    allOpt.textContent = "搜索全部";
    els.scopeSelect.appendChild(allOpt);

    for (const chat of data.chats || []) {
      const opt = document.createElement("option");
      opt.value = String(chat.chat_id);
      opt.textContent = chat.chat_title || `Chat ${chat.chat_id}`;
      els.scopeSelect.appendChild(opt);
    }

    // 尽量保留刷新前选择
    if ([...els.scopeSelect.options].some(o => o.value === currentValue)) {
      els.scopeSelect.value = currentValue;
    } else {
      els.scopeSelect.value = "all";
    }

    setStatus("输入关键词后开始搜索。");
  }

  function _handleMetaFailure(err) {
    setStatus(`读取群列表失败：${err?.message || err}`);
  }

  async function loadMeta() {
    try {
      const data = await _fetchMetaData();
      _handleMetaSuccess(data);
    } catch (err) {
      _handleMetaFailure(err);
    }
  }

  function formatFileSize(bytes) {
    if (bytes == null || Number.isNaN(Number(bytes))) return "";
    const b = Number(bytes);
    if (b < 1024) return `${b}B`;

    const kb = b / 1024;
    if (kb < 1024) return `${kb.toFixed(kb >= 100 ? 0 : 1)}KB`;

    const mb = kb / 1024;
    if (mb < 1024) return `${mb.toFixed(mb >= 100 ? 0 : 1)}MB`;

    const gb = mb / 1024;
    return `${gb.toFixed(gb >= 100 ? 0 : 1)}GB`;
  }

  function typeToLabel(msgType) {
    const t = String(msgType || "").toUpperCase();
    if (t === "TEXT") return "文本";
    if (t === "PHOTO") return "图片";
    if (t === "VIDEO" || t === "GIF" || t === "VIDEO_NOTE") return "视频";
    if (t === "AUDIO" || t === "VOICE") return "音频";
    if (t === "FILE") return "文件";
    return t || "未知";
  }

  function clearResults() {
    els.results.innerHTML = "";
    els.pagination.innerHTML = "";
  }

  function _renderEmptyResults() {
    setStatus("未找到匹配内容。");
    const empty = document.createElement("div");
    empty.className = "empty-box";
    empty.textContent = "没有匹配结果。你可以换关键词，或调整范围/类型。";
    els.results.appendChild(empty);
  }

  function _appendResultLink(container, item) {
    const a = document.createElement("a");
    const hasLink = !!(item.link && String(item.link).trim());
    a.className = "result-link" + (hasLink ? "" : " disabled");
    a.textContent = hasLink ? "查看原消息" : "无可用链接";
    a.setAttribute("aria-label", hasLink ? "查看原消息（新标签页打开）" : "无可用链接");
    if (hasLink) {
      a.href = item.link;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
    } else {
      a.href = "#";
      a.addEventListener("click", (e) => e.preventDefault(), { once: true });
    }
    container.appendChild(a);
  }

  function _buildResultMetaText(item, effectiveSort) {
    const parts = [];
    parts.push(item.msg_date_text || "");
    if (item.chat_title) parts.push(item.chat_title);
    parts.push(typeToLabel(item.msg_type));

    if (effectiveSort === "size" && item.file_size != null) {
      const sizeText = formatFileSize(item.file_size);
      if (sizeText) parts.push(sizeText);
    }

    return parts.filter(Boolean).join(" | ");
  }

  function _buildResultItemElement(item, effectiveSort) {
    const card = document.createElement("div");
    card.className = "result-item";

    const h3 = document.createElement("h3");
    h3.className = "result-title";
    h3.textContent = item.title || "[无文本内容]";
    card.appendChild(h3);

    _appendResultLink(card, item);

    const meta = document.createElement("p");
    meta.className = "result-meta";
    meta.textContent = _buildResultMetaText(item, effectiveSort);
    card.appendChild(meta);

    return card;
  }

  function _renderResultList(items, effectiveSort) {
    for (const item of items) {
      const card = _buildResultItemElement(item, effectiveSort);
      els.results.appendChild(card);
    }
  }

  function renderResults(payload) {
    clearResults();

    const items = payload.items || [];
    const total = Number(payload.total || 0);
    const page = Number(payload.page || 1);
    const totalPages = Number(payload.total_pages || 0);
    const effectiveSort = payload.effective_sort || "time";

    if (total === 0) {
      _renderEmptyResults();
      renderPagination(totalPages, page);
      return;
    }

    setStatus(`共 ${total} 条结果，当前第 ${page} / ${totalPages} 页（每页 100 条）`);
    _renderResultList(items, effectiveSort);
    renderPagination(totalPages, page);
  }

  function createBtn(text, onClick, { disabled = false, ariaLabel = "" } = {}) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn";
    btn.textContent = text;
    btn.disabled = disabled;
    if (ariaLabel) btn.setAttribute("aria-label", ariaLabel);
    btn.addEventListener("click", onClick);
    return btn;
  }

  function _appendSinglePageSummary() {
    const info = document.createElement("span");
    info.className = "page-info";
    info.textContent = "第 1 / 1 页";
    els.pagination.appendChild(info);
  }

  function _shouldRenderComplexPagination(totalPages) {
    return !!totalPages && totalPages > 1;
  }

  function _bindPaginationActions(currentPage, totalPages) {
    if (currentPage > 1) {
      els.pagination.appendChild(
        createBtn("上一页", () => doSearch(currentPage - 1), {
          ariaLabel: "跳转到上一页",
        })
      );
    }

    if (currentPage < totalPages) {
      els.pagination.appendChild(
        createBtn("下一页", () => doSearch(currentPage + 1), {
          ariaLabel: "跳转到下一页",
        })
      );
    }
  }

  function _updatePaginationSummary(currentPage, totalPages) {
    const info = document.createElement("span");
    info.className = "page-info";
    info.textContent = `第 ${currentPage} / ${totalPages} 页`;
    els.pagination.appendChild(info);
  }

  function _createJumpPageElements(currentPage, totalPages) {
    const jumpWrap = document.createElement("div");
    jumpWrap.className = "page-jump-wrap";

    const pageLabel = document.createElement("label");
    pageLabel.className = "sr-only";
    pageLabel.setAttribute("for", "pageJumpInput");
    pageLabel.textContent = "输入页码";

    const input = document.createElement("input");
    input.id = "pageJumpInput";
    input.className = "page-input";
    input.type = "number";
    input.inputMode = "numeric";
    input.min = "1";
    input.max = String(totalPages);
    input.value = String(currentPage);
    input.setAttribute("aria-label", `输入页码，范围 1 到 ${totalPages}`);

    const jumpBtn = createBtn("跳转", () => {}, {
      ariaLabel: "跳转到指定页码",
    });

    return { jumpWrap, pageLabel, input, jumpBtn };
  }

  function _normalizeJumpPage(rawValue, currentPage, totalPages) {
    let p = Number(rawValue);
    if (!Number.isFinite(p)) p = currentPage;
    return Math.max(1, Math.min(totalPages, Math.trunc(p)));
  }

  function _bindJumpPageButtonAction(jumpBtn, input, currentPage, totalPages) {
    jumpBtn.addEventListener("click", () => {
      const p = _normalizeJumpPage(input.value, currentPage, totalPages);
      doSearch(p);
    });
  }

  function _bindJumpPageAction(currentPage, totalPages) {
    const { jumpWrap, pageLabel, input, jumpBtn } = _createJumpPageElements(currentPage, totalPages);
    _bindJumpPageButtonAction(jumpBtn, input, currentPage, totalPages);

    // 这里不拦截 Enter，不做快捷搜索，遵守你的输入习惯
    jumpWrap.appendChild(pageLabel);
    jumpWrap.appendChild(input);
    jumpWrap.appendChild(jumpBtn);
    els.pagination.appendChild(jumpWrap);
  }

  function renderPagination(totalPages, currentPage) {
    els.pagination.innerHTML = "";

    if (!_shouldRenderComplexPagination(totalPages)) {
      if (totalPages === 1) _appendSinglePageSummary();
      return;
    }

    _bindPaginationActions(currentPage, totalPages);
    _updatePaginationSummary(currentPage, totalPages);
    _bindJumpPageAction(currentPage, totalPages);
  }

  function _setSearchLoading(isLoading) {
    setSearching(isLoading);
    if (isLoading) setStatus("正在搜索...");
  }

  function _buildSearchRequestPayload() {
    return {
      query: state.query,
      chat_id: state.chat_id,
      search_type: state.search_type,
      sort_by: state.sort_by,
      order: state.order,
      page: state.page,
    };
  }

  async function _fetchSearchResult(payload) {
    const resp = await fetch("/api/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      let errorMessage = `HTTP ${resp.status}`;
      try {
        const errorPayload = await resp.json();
        if (errorPayload && typeof errorPayload.error === "string" && errorPayload.error.trim()) {
          errorMessage += ` ${errorPayload.error.trim()}`;
        }
      } catch (_ignored) {
        // ignore parse failure and keep HTTP status.
      }
      throw new Error(errorMessage);
    }

    return resp.json();
  }

  function _handleSearchSuccess(data) {
    if (!data.ok) {
      clearResults();
      setStatus(data.error || "搜索失败");
      const box = document.createElement("div");
      box.className = "empty-box";
      box.textContent = data.error || "搜索失败";
      els.results.appendChild(box);
      return;
    }

    renderResults(data);
  }

  function _handleSearchError(err) {
    clearResults();
    const message = `搜索失败：${err?.message || err}`;
    setStatus(message);
    const box = document.createElement("div");
    box.className = "empty-box";
    box.textContent = message;
    els.results.appendChild(box);
  }

  async function doSearch(targetPage) {
    collectFormState({ resetPage: false });

    if (typeof targetPage === "number" && Number.isFinite(targetPage)) {
      state.page = Math.max(1, Math.trunc(targetPage));
    }

    _setSearchLoading(true);
    // 不清空旧结果，避免读屏/视觉闪烁太大，但你也可以改成先清空

    try {
      const payload = _buildSearchRequestPayload();
      const data = await _fetchSearchResult(payload);
      _handleSearchSuccess(data);
    } catch (err) {
      _handleSearchError(err);
    } finally {
      _setSearchLoading(false);
    }
  }

  function bindEvents() {
    els.searchBtn.addEventListener("click", () => {
      collectFormState({ resetPage: true });
      doSearch(1);
    });

    els.typeSelect.addEventListener("change", () => {
      updateSortAvailability();
    });

    // 不在 textarea 上绑定 Enter 搜索，保留回车换行习惯
  }

  async function init() {
    bindEvents();
    updateSortAvailability();
    await loadMeta();
  }

  init();
})();
