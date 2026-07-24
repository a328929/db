"""客户端IP地址获取和验证工具"""

import ipaddress
import os


def get_client_ip(request) -> str:
    """
    获取客户端真实IP地址，优先使用X-Forwarded-For。

    安全考虑：
    1. 只在配置了可信代理时才使用X-Forwarded-For
    2. 从右向左解析，取第一个非内部IP（最后一个代理添加的）
    3. 验证IP格式，防止注入攻击

    Args:
        request: Flask request对象

    Returns:
        客户端IP地址字符串
    """
    # 直连场景：直接使用 remote_addr
    remote_addr = request.remote_addr or "unknown"

    # 检查是否配置了可信代理（通过环境变量）
    # TG_TRUST_PROXY=1 表示信任反向代理
    trust_proxy = os.getenv("TG_TRUST_PROXY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if not trust_proxy:
        return remote_addr

    # 读取 X-Forwarded-For 头部
    forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
    if not forwarded_for:
        return remote_addr

    # X-Forwarded-For 格式: client, proxy1, proxy2
    # 从右向左找第一个非内部IP（即最后一个代理看到的客户端IP）
    ips = [ip.strip() for ip in forwarded_for.split(",")]

    # 反向遍历，跳过内部IP
    for ip in reversed(ips):
        if not ip:
            continue
        # 简单验证IP格式（防止注入）
        if is_valid_ip_format(ip) and not is_internal_ip(ip):
            return ip

    # 如果所有IP都是内部IP或无效，回退到 remote_addr
    return remote_addr


def is_valid_ip_format(ip: str) -> bool:
    """
    验证IP格式是否合法（IPv4或IPv6）

    Args:
        ip: IP地址字符串

    Returns:
        是否为合法IP格式
    """
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False


def is_internal_ip(ip: str) -> bool:
    """
    判断是否为内部IP地址

    Args:
        ip: IP地址字符串

    Returns:
        是否为内部IP（私有地址、回环地址或链路本地地址）
    """
    try:
        addr = ipaddress.ip_address(ip)
        # 检查是否为私有地址或回环地址
        return addr.is_private or addr.is_loopback or addr.is_link_local
    except ValueError:
        return False
