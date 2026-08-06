"""客户端 IP 地址获取和验证工具。"""

import ipaddress
import os

_TRUSTED_PROXY_CIDRS_ENV = "TG_TRUST_PROXY_CIDRS"


def _trusted_proxy_networks() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """Read the explicitly configured reverse-proxy address ranges.

    Invalid entries are ignored so a malformed deployment setting fails closed:
    no request header is trusted unless its direct peer matches a valid network.
    """
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    raw_value = os.getenv(_TRUSTED_PROXY_CIDRS_ENV, "")
    for raw_network in raw_value.split(","):
        candidate = raw_network.strip()
        if not candidate:
            continue
        try:
            networks.append(ipaddress.ip_network(candidate, strict=False))
        except ValueError:
            continue
    return tuple(networks)


def _is_trusted_proxy(
    address: str,
    networks: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...],
) -> bool:
    try:
        parsed_address = ipaddress.ip_address(address)
    except ValueError:
        return False
    return any(parsed_address in network for network in networks)


def get_client_ip(request) -> str:
    """Return the client address while trusting XFF only from known proxies.

    ``TG_TRUST_PROXY_CIDRS`` accepts a comma-separated list of IP addresses or
    CIDRs. When it is empty, malformed, or the immediate peer is not in one of
    those networks, request headers are ignored. This deliberately replaces the
    former boolean trust switch, which allowed direct clients to forge XFF.
    """
    remote_addr = request.remote_addr or "unknown"
    trusted_networks = _trusted_proxy_networks()
    if not trusted_networks or not _is_trusted_proxy(remote_addr, trusted_networks):
        return remote_addr

    forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
    if not forwarded_for:
        return remote_addr

    # XFF is client, proxy1, proxy2. Strip only proxy hops configured above;
    # the first remaining valid address is the address seen by the trusted edge.
    for candidate in reversed(forwarded_for.split(",")):
        candidate = candidate.strip()
        if not candidate or not is_valid_ip_format(candidate):
            continue
        if _is_trusted_proxy(candidate, trusted_networks):
            continue
        return candidate

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
