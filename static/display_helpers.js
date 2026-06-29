(() => {
  "use strict";

  const SHANGHAI_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  function parseUtcDateTime(value) {
    const text = String(value || "").trim();
    if (!text) {
      return null;
    }

    const normalized = text.replace("T", " ").replace(/\s+UTC$/i, "");
    const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized);
    let candidate = normalized.replace(" ", "T");
    if (hasTimezone) {
      candidate = candidate.replace(/(\.\d+)(?=Z|[+-]\d{2}:?\d{2}$)/i, "");
    } else {
      candidate = candidate.replace(/\.\d+$/, "");
      candidate += "Z";
    }

    const parsed = new Date(candidate);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed;
  }

  function formatShanghaiDateTime(date) {
    const parts = SHANGHAI_DATE_TIME_FORMATTER.formatToParts(date);
    const values = {};
    parts.forEach((part) => {
      values[part.type] = part.value;
    });
    if (
      !values.year ||
      !values.month ||
      !values.day ||
      !values.hour ||
      !values.minute ||
      !values.second
    ) {
      return SHANGHAI_DATE_TIME_FORMATTER.format(date).replace(/\//g, "-");
    }
    return `${values.year}-${values.month}-${values.day} ${values.hour}:${values.minute}:${values.second}`;
  }

  function formatDateTime(value) {
    const parsed = parseUtcDateTime(value);
    if (!parsed) {
      const fallback = String(value || "").trim();
      return fallback || "暂无";
    }
    return formatShanghaiDateTime(parsed);
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

  function formatDuration(seconds) {
    if (seconds == null || Number.isNaN(Number(seconds)) || seconds < 0) return "";
    const s = Math.floor(Number(seconds));
    const hours = Math.floor(s / 3600);
    const minutes = Math.floor((s % 3600) / 60);
    const secs = s % 60;

    const parts = [];
    if (hours > 0) {
      parts.push(hours);
      parts.push(String(minutes).padStart(2, "0"));
    } else {
      parts.push(minutes);
    }
    parts.push(String(secs).padStart(2, "0"));
    return parts.join(":");
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

  function appendBadge(parent, className, text) {
    const badge = document.createElement("span");
    badge.className = className;
    badge.textContent = text;
    parent.appendChild(badge);
  }

  function appendMessageBadges(parent, item) {
    if (item.is_promo) {
      appendBadge(parent, "promo-badge", "广告");
    }

    const mt = String(item.msg_type || "TEXT").toUpperCase();
    if (mt === "PHOTO") appendBadge(parent, "badge badge-photo", "图片");
    else if (mt === "VIDEO" || mt === "GIF" || mt === "VIDEO_NOTE") {
      appendBadge(parent, "badge badge-video", "视频");
    } else if (mt === "AUDIO" || mt === "VOICE") {
      appendBadge(parent, "badge badge-audio", "音频");
    } else if (mt === "FILE") {
      appendBadge(parent, "badge badge-file", "文件");
    }
  }

  function appendBadgesAndText(parent, item, text) {
    appendMessageBadges(parent, item);
    parent.appendChild(document.createTextNode(text || "[无文本内容]"));
  }

  window.TgHarvestDisplay = {
    appendBadgesAndText,
    formatDateTime,
    formatDuration,
    formatFileSize,
    typeToLabel,
  };
})();
