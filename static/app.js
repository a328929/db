(() => {
  "use strict";

  const display = window.TgHarvestDisplay;

  const state = {
    query: "",
    chat_id: "all",
    search_type: "all",
    sort_by: "time",
    order: "desc",
    start_date: "",
    end_date: "",
    page: 1,
  };
  const SEARCH_COUNT_CACHE_MAX_ENTRIES = 200;
  const searchCountCache = new Map();

  const els = {
    queryInput: document.getElementById("queryInput"),
    searchBtn: document.getElementById("searchBtn"),
    scopeSelect: document.getElementById("scopeSelect"),
    typeSelect: document.getElementById("typeSelect"),
    sortSelect: document.getElementById("sortSelect"),
    orderSelect: document.getElementById("orderSelect"),
    startDateInput: document.getElementById("startDateInput"),
    endDateInput: document.getElementById("endDateInput"),
    clearStartDateBtn: document.getElementById("clearStartDateBtn"),
    clearEndDateBtn: document.getElementById("clearEndDateBtn"),
    statusLine: document.getElementById("statusLine"),
    groupFacets: document.getElementById("groupFacets"),
    results: document.getElementById("results"),
    pagination: document.getElementById("pagination"),
  };

  function setStatus(text) {
    els.statusLine.textContent = text;
  }

  function setSearching(isSearching) {
    els.searchBtn.disabled = isSearching;
    els.searchBtn.textContent = isSearching ? "搜索中..." : "搜索";

    els.scopeSelect.disabled = isSearching;
    els.typeSelect.disabled = isSearching;
    els.sortSelect.disabled = isSearching;
    els.orderSelect.disabled = isSearching;
    els.startDateInput.disabled = isSearching;
    els.endDateInput.disabled = isSearching;
    els.clearStartDateBtn.disabled = isSearching;
    els.clearEndDateBtn.disabled = isSearching;
    els.queryInput.disabled = isSearching;
  }

  function updateDateClearButtons() {
    els.clearStartDateBtn.hidden = !(els.startDateInput.value || "").trim();
    els.clearEndDateBtn.hidden = !(els.endDateInput.value || "").trim();
  }

  function clearDateInput(input, clearButton) {
    if (!(input.value || "").trim()) {
      clearButton.hidden = true;
      return;
    }
    input.value = "";
    clearButton.hidden = true;
    input.focus();
    _markSearchCriteriaDirty();
  }

  function collectFormState({ resetPage = false } = {}) {
    state.query = els.queryInput.value || "";
    state.chat_id = els.scopeSelect.value || "all";
    state.search_type = els.typeSelect.value || "all";
    state.sort_by = els.sortSelect.value || "time";
    state.order = els.orderSelect.value || "desc";
    state.start_date = (els.startDateInput.value || "").trim();
    state.end_date = (els.endDateInput.value || "").trim();
    if (resetPage) state.page = 1;
  }

  function _decideSortAvailability(typeValue, queryValue, currentSortValue) {
    const disableSize = typeValue === "all" || typeValue === "text";
    const disableDuration = typeValue === "all" || typeValue === "text" || typeValue === "image";
    const disableRelevance = !(queryValue || "").trim();

    let shouldFallback = false;
    if (disableSize && currentSortValue === "size") shouldFallback = true;
    if (disableDuration && currentSortValue === "duration") shouldFallback = true;
    if (disableRelevance && currentSortValue === "relevance") shouldFallback = true;

    return {
      disableSize,
      disableDuration,
      disableRelevance,
      shouldFallback,
      fallbackValue: "time",
    };
  }

  function _applySortAvailabilityUI(sizeOption, durationOption, relevanceOption, sortSelect, decision) {
    if (sizeOption) sizeOption.disabled = decision.disableSize;
    if (durationOption) durationOption.disabled = decision.disableDuration;
    if (relevanceOption) relevanceOption.disabled = decision.disableRelevance;

    // 附加逻辑：当前排序不可用时切回时间
    if (decision.shouldFallback) {
      sortSelect.value = decision.fallbackValue;
    }
  }

  function updateSortAvailability() {
    const typeValue = els.typeSelect.value;
    const queryValue = els.queryInput.value;
    const currentSortValue = els.sortSelect.value;
    const sizeOption = els.sortSelect.querySelector('option[value="size"]');
    const durationOption = els.sortSelect.querySelector('option[value="duration"]');
    const relevanceOption = els.sortSelect.querySelector('option[value="relevance"]');

    const decision = _decideSortAvailability(typeValue, queryValue, currentSortValue);
    _applySortAvailabilityUI(sizeOption, durationOption, relevanceOption, els.sortSelect, decision);
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

    const currentValue = els.scopeSelect.value || "all";
    els.scopeSelect.textContent = "";

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
    els.results.textContent = "";
    els.pagination.textContent = "";
    els.groupFacets.textContent = "";
  }

  function ensureScopeOption(chatId, chatTitle) {
    const value = String(chatId);
    const exists = [...els.scopeSelect.options].some((option) => option.value === value);
    if (exists) return;
    const option = document.createElement("option");
    option.value = value;
    option.textContent = chatTitle || `Chat ${value}`;
    els.scopeSelect.appendChild(option);
  }

  function renderGroupFacets(facets) {
    els.groupFacets.textContent = "";
    if (state.chat_id !== "all" || !Array.isArray(facets) || facets.length === 0) {
      return;
    }

    for (const facet of facets) {
      const chatId = facet && facet.chat_id;
      if (chatId === undefined || chatId === null) continue;
      const chatTitle = String(facet.chat_title || `Chat ${chatId}`);
      const count = Number(facet.count || 0);
      if (!Number.isFinite(count) || count <= 0) continue;

      const button = document.createElement("button");
      button.type = "button";
      button.className = "group-facet-btn";
      button.setAttribute("aria-label", `仅查看 ${chatTitle} 中的 ${count} 条命中结果`);

      const titleSpan = document.createElement("span");
      titleSpan.textContent = chatTitle;
      button.appendChild(titleSpan);

      const countSpan = document.createElement("span");
      countSpan.className = "group-facet-count";
      countSpan.textContent = String(count);
      button.appendChild(countSpan);

      button.addEventListener("click", () => {
        ensureScopeOption(chatId, chatTitle);
        els.scopeSelect.value = String(chatId);
        collectFormState({ resetPage: true });
        doSearch(1);
      });

      els.groupFacets.appendChild(button);
    }
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

    const hasLink = !!(item.link && String(item.link).trim());
    const a = document.createElement(hasLink ? "a" : "span");
    a.className = "result-link" + (hasLink ? "" : " disabled");
    a.textContent = hasLink ? "查看原消息" : "无可用链接";
    a.setAttribute("aria-label", hasLink ? "查看原消息（新标签页打开）" : "无可用链接");
    if (hasLink) {
      a.href = item.link;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
    } else {
      a.setAttribute("aria-disabled", "true");
    }
    wrap.appendChild(a);

    const ctxLink = document.createElement("a");
    ctxLink.className = "result-link context-link";
    ctxLink.textContent = "本地上下文";
    ctxLink.setAttribute("aria-label", "在本地数据库中查看该消息的聊天上下文");
    ctxLink.href = `/chat/${item.chat_id}?msg_id=${item.message_id}`;
    ctxLink.target = "_blank";
    ctxLink.rel = "noopener noreferrer";

    wrap.appendChild(ctxLink);

    container.appendChild(wrap);
  }

  function _buildResultMetaText(item, effectiveSort) {
    const parts = [];
    parts.push(display.formatDateTime(item.msg_date_text));
    if (item.chat_title) parts.push(item.chat_title);
    parts.push(display.typeToLabel(item.msg_type));

    if (effectiveSort === "size" && item.file_size != null) {
      const sizeText = display.formatFileSize(item.file_size);
      if (sizeText) parts.push(sizeText);
    }

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
    const fragment = document.createDocumentFragment();
    for (const item of items) {
      const card = _buildResultItemElement(item, effectiveSort);
      fragment.appendChild(card);
    }
    els.results.appendChild(fragment);
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
      setStatus(`已快速加载 ${items.length} 条结果，正在后台统计结果数量...`);
    } else {
      if (total === 0 && items.length === 0) {
        setStatus("未找到匹配内容。");
      } else {
        setStatus(_formatResultSummary(payload));
      }
    }

    _renderResultList(items, effectiveSort);
    renderGroupFacets(payload.chat_facets);

    if (!isCounting) {
      renderPagination(totalPages, page);
    } else {
      setPaginationInfo("正在计算页码...");
    }
  }

  function applyCountToRenderedResults(payload) {
    const items = payload.items || [];
    const total = Number(payload.total || 0);
    const page = Number(payload.page || 1);
    const totalPages = Number(payload.total_pages || 0);

    if (total === 0 && items.length === 0 && els.results.children.length === 0) {
      _renderEmptyResults();
      renderPagination(totalPages, page);
      return;
    }

    if (total === 0 && items.length === 0) {
      setStatus("未找到匹配内容。");
    } else {
      setStatus(_formatResultSummary(payload));
    }

    renderGroupFacets(payload.chat_facets);
    renderPagination(totalPages, page);
  }

  function _formatResultSummary(payload) {
    const total = Number(payload.total || 0);
    const page = Number(payload.page || 1);
    const totalPages = Number(payload.total_pages || 0);
    const pageSize = Number(payload.page_size || 50);
    if (payload.total_is_capped) {
      return `超过 ${total} 条结果，当前第 ${page} / 至少 ${totalPages} 页（每页 ${pageSize} 条）`;
    }
    return `共 ${total} 条结果，当前第 ${page} / ${totalPages} 页（每页 ${pageSize} 条）`;
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
    setPaginationInfo("第 1 / 1 页");
  }

  function setPaginationInfo(text) {
    const info = document.createElement("span");
    info.className = "page-info";
    info.textContent = text;
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
    setPaginationInfo(`第 ${currentPage} / ${totalPages} 页`);
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

    jumpWrap.appendChild(pageLabel);
    jumpWrap.appendChild(input);
    jumpWrap.appendChild(jumpBtn);
    els.pagination.appendChild(jumpWrap);
  }

  function renderPagination(totalPages, currentPage) {
    els.pagination.textContent = "";

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
      start_date: state.start_date,
      end_date: state.end_date,
      page: state.page,
    };
  }

  function _buildCountCacheKey(dataVersion) {
    return JSON.stringify({
      query: state.query,
      chat_id: state.chat_id,
      search_type: state.search_type,
      start_date: state.start_date,
      end_date: state.end_date,
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

    const cacheKey = _buildCountCacheKey(dataVersion);
    // Map preserves insertion order. Refresh existing keys and evict the
    // least-recently-used entry so long-lived tabs remain bounded.
    searchCountCache.delete(cacheKey);
    searchCountCache.set(cacheKey, {
      total,
      total_pages: totalPages,
      total_is_capped: !!payload.total_is_capped,
      chat_facets: Array.isArray(payload.chat_facets) ? payload.chat_facets : [],
    });
    while (searchCountCache.size > SEARCH_COUNT_CACHE_MAX_ENTRIES) {
      const oldestKey = searchCountCache.keys().next().value;
      searchCountCache.delete(oldestKey);
    }
  }

  function _getCachedCount(dataVersion) {
    const version = String(dataVersion || "");
    if (!version) return null;
    const cacheKey = _buildCountCacheKey(version);
    const cached = searchCountCache.get(cacheKey) || null;
    if (cached) {
      searchCountCache.delete(cacheKey);
      searchCountCache.set(cacheKey, cached);
    }
    return cached;
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
    els.pagination.textContent = "";
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
        data.chat_facets = countData.chat_facets || [];
        data.data_version = countData.data_version || data.data_version;
        _setCachedCount(data);
        applyCountToRenderedResults(data);
      } else {
        setStatus("后台统计暂不可用，仅显示当前页。");
        els.pagination.textContent = "";
      }
    } catch (_err) {
      if (searchId !== currentSearchId) return;
      setStatus("后台统计失败，仅显示当前页。");
      els.pagination.textContent = "";
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

      if ((!data.items || data.items.length === 0) && Number(data.total || 0) <= 0) {
        data.total = 0;
        data.total_pages = 0;
        _setCachedCount(data);
        _handleSearchSuccess(data, false);
        return;
      }

      // Manticore returns the match total with the page query itself.  Reuse
      // it directly; only the SQLite browse path needs a deferred count call.
      const hasManticoreCount =
        data.search_backend === "manticore" && Number.isFinite(Number(data.total)) &&
        Number(data.total) >= 0;
      if (hasManticoreCount) {
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
        data.chat_facets = cachedCount.chat_facets || [];
        applyCountToRenderedResults(data);
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
      updateSortAvailability();
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

    els.startDateInput.addEventListener("input", () => {
      updateDateClearButtons();
      _markSearchCriteriaDirty();
    });

    els.endDateInput.addEventListener("input", () => {
      updateDateClearButtons();
      _markSearchCriteriaDirty();
    });

    els.clearStartDateBtn.addEventListener("click", () => {
      clearDateInput(els.startDateInput, els.clearStartDateBtn);
    });

    els.clearEndDateBtn.addEventListener("click", () => {
      clearDateInput(els.endDateInput, els.clearEndDateBtn);
    });

    // 不在 textarea 上绑定 Enter 搜索，保留回车换行习惯
  }

  async function init() {
    bindEvents();
    updateSortAvailability();
    updateDateClearButtons();
    await loadMeta();
  }

  init();
})();
