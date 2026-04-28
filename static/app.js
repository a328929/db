(() => {
  "use strict";

  const display = window.TgHarvestDisplay;

  const state = {
    query: "",
    chat_id: "all",
    search_type: "all",
    sort_by: "time",
    order: "desc",
    page: 1,
  };
  const searchCountCache = new Map();

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
    
    // 同时也禁用所有下拉框，防止用户在搜索过程中频繁切换导致状态混乱
    els.scopeSelect.disabled = isSearching;
    els.typeSelect.disabled = isSearching;
    els.sortSelect.disabled = isSearching;
    els.orderSelect.disabled = isSearching;
    els.queryInput.disabled = isSearching;
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
    const disableDuration = typeValue === "all" || typeValue === "text" || typeValue === "image";
    
    let shouldFallback = false;
    if (disableSize && currentSortValue === "size") shouldFallback = true;
    if (disableDuration && currentSortValue === "duration") shouldFallback = true;

    return {
      disableSize,
      disableDuration,
      shouldFallback,
      fallbackValue: "time",
    };
  }

  function _applySortAvailabilityUI(sizeOption, durationOption, sortSelect, decision) {
    if (sizeOption) sizeOption.disabled = decision.disableSize;
    if (durationOption) durationOption.disabled = decision.disableDuration;

    // 附加逻辑：当前排序不可用时切回时间
    if (decision.shouldFallback) {
      sortSelect.value = decision.fallbackValue;
    }
  }

  function updateSortAvailability() {
    const typeValue = els.typeSelect.value;
    const currentSortValue = els.sortSelect.value;
    const sizeOption = els.sortSelect.querySelector('option[value="size"]');
    const durationOption = els.sortSelect.querySelector('option[value="duration"]');

    const decision = _decideSortAvailability(typeValue, currentSortValue);
    _applySortAvailabilityUI(sizeOption, durationOption, els.sortSelect, decision);
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
    const wrap = document.createElement("div");
    wrap.className = "result-links-wrap";

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
    wrap.appendChild(a);

    // 新增：本地上下文入口
    const ctxLink = document.createElement("a");
    ctxLink.className = "result-link context-link";
    ctxLink.textContent = "本地上下文";
    ctxLink.setAttribute("aria-label", "在本地数据库中查看该消息的聊天上下文");
    ctxLink.href = `/chat/${item.chat_id}?msg_id=${item.message_id}`;
    ctxLink.target = "_blank";

    wrap.appendChild(ctxLink);

    container.appendChild(wrap);
  }

  function _buildResultMetaText(item, effectiveSort) {
    const parts = [];
    parts.push(item.msg_date_text || "");
    if (item.chat_title) parts.push(item.chat_title);
    parts.push(display.typeToLabel(item.msg_type));

    if (effectiveSort === "size" && item.file_size != null) {
      const sizeText = display.formatFileSize(item.file_size);
      if (sizeText) parts.push(sizeText);
    }
    
    // 如果有时长信息，始终显示在元数据中
    if (item.duration_sec != null) {
        const durText = display.formatDuration(item.duration_sec);
        if (durText) parts.push(durText);
    }

    return parts.filter(Boolean).join(" | ");
  }

  function _appendResultTitleContent(titleEl, item) {
    display.appendBadgesAndText(titleEl, item, item.title);
  }

  function _buildResultItemElement(item, effectiveSort) {
    const card = document.createElement("div");
    card.className = "result-item";
    if (item.is_promo) card.classList.add("is-promo");

    const h3 = document.createElement("h3");
    h3.className = "result-title";
    _appendResultTitleContent(h3, item);
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

  function renderResults(payload, isCounting = false) {
    clearResults();

    const items = payload.items || [];
    const total = Number(payload.total || 0);
    const page = Number(payload.page || 1);
    const totalPages = Number(payload.total_pages || 0);
    const effectiveSort = payload.effective_sort || "time";

    if (total === 0 && !isCounting && items.length === 0) {
      _renderEmptyResults();
      renderPagination(totalPages, page);
      return;
    }

    if (isCounting) {
      setStatus(`已快速加载 ${items.length} 条结果，正在后台精确统计总数...`);
    } else {
      if (total === 0 && items.length === 0) {
        setStatus("未找到匹配内容。");
      } else {
        setStatus(`共 ${total} 条结果，当前第 ${page} / ${totalPages} 页（每页 100 条）`);
      }
    }
    
    _renderResultList(items, effectiveSort);
    
    if (!isCounting) {
      renderPagination(totalPages, page);
    } else {
      els.pagination.innerHTML = '<span class="page-info">正在计算页码...</span>';
    }
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

  function _buildCountCacheKey(dataVersion) {
    return JSON.stringify({
      query: state.query,
      chat_id: state.chat_id,
      search_type: state.search_type,
      sort_by: state.sort_by,
      order: state.order,
      data_version: String(dataVersion || ""),
    });
  }

  function _setCachedCount(payload) {
    const total = Number(payload.total);
    const totalPages = Number(payload.total_pages);
    if (!Number.isFinite(total) || total < 0) return;
    if (!Number.isFinite(totalPages) || totalPages < 0) return;
    const dataVersion = String(payload.data_version || "");
    if (!dataVersion) return;

    searchCountCache.set(_buildCountCacheKey(dataVersion), {
      total,
      total_pages: totalPages,
      total_is_capped: !!payload.total_is_capped,
    });
  }

  function _getCachedCount(dataVersion) {
    const version = String(dataVersion || "");
    if (!version) return null;
    return searchCountCache.get(_buildCountCacheKey(version)) || null;
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
      let errorMessage = `错误 ${resp.status}: `;
      
      // 修复：针对特定状态码提供业务化中文提示
      if (resp.status === 429) {
          errorMessage = "搜索太频繁啦，请稍等一分钟再试。";
      } else if (resp.status === 500 || resp.status === 503) {
          errorMessage = "数据库忙或采集任务正在进行中，请 15 秒后再试。";
      } else {
          try {
            const errorPayload = await resp.json();
            if (errorPayload && typeof errorPayload.error === "string") {
              errorMessage += errorPayload.error;
            } else {
              errorMessage += "系统响应异常";
            }
          } catch (_ignored) {
            errorMessage += "未知错误";
          }
      }
      throw new Error(errorMessage);
    }

    return resp.json();
  }

  function _handleSearchSuccess(data, isCounting = false) {
    if (!data.ok) {
      clearResults();
      setStatus(data.error || "搜索失败");
      const box = document.createElement("div");
      box.className = "empty-box";
      box.textContent = data.error || "搜索失败";
      els.results.appendChild(box);
      return;
    }

    renderResults(data, isCounting);
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

  let currentSearchId = 0;

  function _markSearchCriteriaDirty() {
    currentSearchId += 1;
    els.pagination.innerHTML = "";
    setStatus("搜索条件已更改，点击搜索重新查询。");
  }

  async function _fetchAndApplyCountInBackground(searchId, data) {
    try {
      const countPayload = _buildSearchRequestPayload();
      countPayload.count_only = true;
      const countData = await _fetchSearchResult(countPayload);

      if (searchId !== currentSearchId) return;

      if (countData.ok) {
        data.total = countData.total;
        data.total_pages = countData.total_pages;
        data.total_is_capped = countData.total_is_capped;
        data.data_version = countData.data_version || data.data_version;
        _setCachedCount(data);
        _handleSearchSuccess(data, false);
      } else {
        setStatus("后台统计暂不可用，仅显示当前页。");
        els.pagination.innerHTML = "";
      }
    } catch (_err) {
      if (searchId !== currentSearchId) return;
      setStatus("后台统计失败，仅显示当前页。");
      els.pagination.innerHTML = "";
    }
  }

  async function doSearch(targetPage) {
    const searchId = ++currentSearchId;
    let loadingReleased = false;
    collectFormState({ resetPage: false });

    if (typeof targetPage === "number" && Number.isFinite(targetPage)) {
      state.page = Math.max(1, Math.trunc(targetPage));
    }

    _setSearchLoading(true);

    try {
      const payload = _buildSearchRequestPayload();
      
      payload.skip_count = true;
      const data = await _fetchSearchResult(payload);
      
      if (searchId !== currentSearchId) return;

      if (!data.items || data.items.length === 0) {
        data.total = 0;
        data.total_pages = 0;
        _setCachedCount(data);
        _handleSearchSuccess(data, false);
        return;
      }

      _handleSearchSuccess(data, true);

      const cachedCount = _getCachedCount(data.data_version);
      if (cachedCount) {
        data.total = cachedCount.total;
        data.total_pages = cachedCount.total_pages;
        data.total_is_capped = cachedCount.total_is_capped;
        _handleSearchSuccess(data, false);
        return;
      }

      _setSearchLoading(false);
      loadingReleased = true;
      _fetchAndApplyCountInBackground(searchId, { ...data });

    } catch (err) {
      if (searchId === currentSearchId) {
        _handleSearchError(err);
      }
    } finally {
      // 仅在未提前释放交互锁时才在这里恢复 UI。
      if (!loadingReleased && searchId === currentSearchId) {
        _setSearchLoading(false);
      }
    }
  }

  function bindEvents() {
    els.searchBtn.addEventListener("click", () => {
      collectFormState({ resetPage: true });
      doSearch(1);
    });

    els.queryInput.addEventListener("input", () => {
      _markSearchCriteriaDirty();
    });

    els.scopeSelect.addEventListener("change", () => {
      _markSearchCriteriaDirty();
    });

    els.typeSelect.addEventListener("change", () => {
      updateSortAvailability();
      _markSearchCriteriaDirty();
    });

    els.sortSelect.addEventListener("change", () => {
      collectFormState({ resetPage: true });
      doSearch(1);
    });

    els.orderSelect.addEventListener("change", () => {
      collectFormState({ resetPage: true });
      doSearch(1);
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
