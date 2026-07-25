/**
 * 日期和时间格式化工具函数
 * 提供 UTC 时间解析和上海时区格式化功能
 */
(function () {
  'use strict';

  var SHANGHAI_DATE_TIME_FORMATTER = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });

  /**
   * 解析 UTC 时间字符串为 Date 对象
   * @param {string|null|undefined} value - UTC 时间字符串
   * @returns {Date|null} 解析后的 Date 对象，失败返回 null
   */
  function parseUtcDateTime(value) {
    var text = String(value || '').trim();
    if (!text) {
      return null;
    }

    var normalized = text.replace('T', ' ').replace(/\s+UTC$/i, '');
    var hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized);
    var candidate = normalized.replace(' ', 'T');
    if (hasTimezone) {
      candidate = candidate.replace(/(\.\d+)(?=Z|[+-]\d{2}:?\d{2}$)/i, '');
    } else {
      candidate = candidate.replace(/\.\d+$/, '');
      candidate += 'Z';
    }

    var parsed = new Date(candidate);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed;
  }

  /**
   * 将 Date 对象格式化为上海时区的日期时间字符串
   * @param {Date} date - Date 对象
   * @returns {string} 格式化后的字符串 (YYYY-MM-DD HH:mm:ss)
   */
  function formatShanghaiDateTime(date) {
    var parts = SHANGHAI_DATE_TIME_FORMATTER.formatToParts(date);
    var values = {};
    parts.forEach(function (part) {
      values[part.type] = part.value;
    });
    if (
      !values.year
      || !values.month
      || !values.day
      || !values.hour
      || !values.minute
      || !values.second
    ) {
      return SHANGHAI_DATE_TIME_FORMATTER.format(date).replace(/\//g, '-');
    }
    return values.year + '-' + values.month + '-' + values.day + ' '
      + values.hour + ':' + values.minute + ':' + values.second;
  }

  /**
   * 格式化时间字符串为上海时区显示
   * @param {string|null|undefined} value - UTC 时间字符串
   * @returns {string} 格式化后的上海时区时间，失败返回原值或"暂无"
   */
  function formatDateTime(value) {
    var parsed = parseUtcDateTime(value);
    if (!parsed) {
      var fallback = String(value || '').trim();
      return fallback || '暂无';
    }
    return formatShanghaiDateTime(parsed);
  }

  // 导出到全局作用域
  window.TgHarvestDateUtils = {
    parseUtcDateTime: parseUtcDateTime,
    formatShanghaiDateTime: formatShanghaiDateTime,
    formatDateTime: formatDateTime
  };
})();
