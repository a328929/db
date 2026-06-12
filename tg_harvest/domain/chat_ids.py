

def stored_chat_id_from_entity_id(entity_id: int) -> int:
    """Return the positive chat id shape used by the local database."""
    value = abs(int(entity_id))
    raw = str(value)
    if raw.startswith("100") and len(raw) > 3:
        return int(raw[3:])
    return value


def candidate_chat_entity_ids(chat_id: int) -> list[int]:
    """Return Telethon entity ids worth trying for a stored chat id."""
    raw = int(chat_id)
    candidates: list[int] = [raw]
    abs_id = abs(raw)

    if raw > 0:
        candidates.extend((int(f"-100{raw}"), -raw))
    else:
        raw_text = str(abs_id)
        if raw_text.startswith("100") and len(raw_text) > 3:
            stripped = int(raw_text[3:])
            candidates.append(stripped)
            candidates.append(-stripped)
        else:
            candidates.append(int(f"-100{abs_id}"))
        candidates.append(abs_id)

    deduped: list[int] = []
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped
