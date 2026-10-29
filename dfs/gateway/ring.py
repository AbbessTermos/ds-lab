import bisect, hashlib


def _h(s):
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


class Ring:
    """Consistent hashing ring with virtual nodes."""

    def __init__(self, nodes, vnodes=100):
        self.points = sorted((_h(f"{n}#{i}"), n) for n in nodes for i in range(vnodes))
        self.keys = [p[0] for p in self.points]
        self.nodes = set(nodes)

    def lookup(self, key, n):
        """Return n distinct nodes clockwise from hash(key)."""
        out = []
        if not self.points:
            return out
        i = bisect.bisect(self.keys, _h(key))
        for j in range(len(self.points)):
            node = self.points[(i + j) % len(self.points)][1]
            if node not in out:
                out.append(node)
                if len(out) == n:
                    break
        return out
