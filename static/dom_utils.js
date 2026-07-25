/**
 * DOM 操作工具函数
 * 提供常用的 DOM 创建和操作辅助函数
 */
(function () {
  'use strict';

  /**
   * 创建带文本内容的元素
   * @param {string} tagName - HTML 标签名
   * @param {string} className - CSS 类名（可选）
   * @param {string} text - 文本内容
   * @returns {HTMLElement} 创建的元素
   */
  function createTextElement(tagName, className, text) {
    var el = document.createElement(tagName);
    if (className) el.className = className;
    el.textContent = String(text || '');
    return el;
  }

  /**
   * 创建信息标签组件（label + value 样式）
   * @param {string} label - 标签文本
   * @param {string} value - 值文本
   * @param {string} pillClass - 外层容器的 CSS 类名（默认 'info-pill'）
   * @param {string} labelClass - 标签的 CSS 类名（默认 'info-label'）
   * @returns {HTMLElement} 创建的信息标签元素
   */
  function createInfoPill(label, value, pillClass, labelClass) {
    var pill = document.createElement('span');
    pill.className = pillClass || 'info-pill';
    pill.appendChild(createTextElement('span', labelClass || 'info-label', label));
    pill.appendChild(createTextElement('strong', '', value || '暂无'));
    return pill;
  }

  /**
   * 创建频道信息标签（admin_channels.js 风格）
   * @param {string} label - 标签文本
   * @param {string} value - 值文本
   * @returns {HTMLElement} 创建的频道信息标签元素
   */
  function createChannelInfoPill(label, value) {
    return createInfoPill(label, value, 'channel-info-pill', 'channel-info-label');
  }

  /**
   * 创建恢复信息标签（admin_recovery.js 风格）
   * @param {string} label - 标签文本
   * @param {string} value - 值文本
   * @returns {HTMLElement} 创建的恢复信息标签元素
   */
  function createRecoveryInfoPill(label, value) {
    return createInfoPill(label, value, 'recovery-info-pill', 'recovery-info-label');
  }

  // 导出到全局作用域
  window.TgHarvestDomUtils = {
    createTextElement: createTextElement,
    createInfoPill: createInfoPill,
    createChannelInfoPill: createChannelInfoPill,
    createRecoveryInfoPill: createRecoveryInfoPill
  };
})();
