#!/usr/bin/env python3
"""Two-account end-to-end test of the group write path against production.

Drives two `auxin-ng jsonrpc` daemons (account A creates, account B is the
other side) through every group RPC in both directions, asserting on the
server-signed change log at each step. Creates a throwaway group and leaves
it at the end. Real Signal traffic: only run with test accounts.

    scripts/group-e2e.py --a-args "" --b-args "-a test2"
"""
import argparse, base64, json, os, queue, subprocess, sys, tempfile, threading, time, zlib, struct

BIN = os.environ.get("AUXIN_NG", os.path.join(os.path.dirname(__file__), "..", "target", "debug", "auxin-ng"))


class RpcError(Exception):
    pass


class Node:
    def __init__(self, name, extra_args):
        self.name = name
        self.proc = subprocess.Popen(
            [BIN, *extra_args, "jsonrpc"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        self.responses, self.events = {}, queue.Queue()
        self.next_id = 1
        self.lock = threading.Lock()
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self.proc.stdout:
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "id" in msg and msg["id"] is not None:
                with self.lock:
                    self.responses[msg["id"]] = msg
            else:
                self.events.put(msg)

    def call(self, method, timeout=90, **params):
        with self.lock:
            rid = self.next_id
            self.next_id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                msg = self.responses.pop(rid, None)
            if msg:
                if "error" in msg:
                    raise RpcError(msg["error"]["message"])
                return msg["result"]
            time.sleep(0.05)
        raise TimeoutError(f"{self.name}.{method} timed out")

    def wait_event(self, pred, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                ev = self.events.get(timeout=0.5)
            except queue.Empty:
                continue
            if pred(ev):
                return ev
        raise TimeoutError(f"{self.name}: no matching event")

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=30)


def png(w, h, rgb):
    raw = b"".join(b"\x00" + bytes(rgb) * w for _ in range(h))
    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


passed, failed = [], []


def step(name):
    def wrap(fn):
        def run(*a, **k):
            try:
                out = fn(*a, **k)
                passed.append(name)
                print(f"PASS {name}")
                return out
            except Exception as e:  # noqa: BLE001
                failed.append((name, e))
                print(f"FAIL {name}: {e}")
                raise
        return run
    return wrap


def history_types(node, key):
    return [(e["revision"], [c["type"] for c in e["changes"]]) for e in node.call("getGroupHistory", group=key)["history"]]


def last_change(node, key):
    return history_types(node, key)[-1]


def is_group_message(ev, key):
    # Proto-JSON renders bytes fields as base64, not hex.
    key_b64 = base64.b64encode(bytes.fromhex(key)).decode()
    return ev.get("method") == "message" and ev["params"].get("content", {}).get("groupV2", {}).get("masterKey") == key_b64


def leave_stray_groups(node, prefix="e2e"):
    """Leaves groups left behind by earlier aborted runs."""
    for g in node.call("listGroups"):
        if g["title"].startswith(prefix) and any(m["uuid"] for m in g["members"]):
            try:
                node.call("leaveGroup", group=g["masterKey"], force=True)
                print(f"cleanup: {node.name} left stray group {g['title']!r}")
            except RpcError as e:
                print(f"cleanup: {node.name} could not leave {g['title']!r}: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a-args", default="")
    ap.add_argument("--b-args", default="-a test2")
    ap.add_argument("--a-db", default="~/Library/Application Support/io.auxin.auxin-ng/auxin.db3",
                    help="A's sqlite store, for forgetting B's profile key")
    ap.add_argument("--keep-contact", action="store_true",
                    help="don't forget B's profile key in A's store (skips the pending-invite path)")
    args = ap.parse_args()

    # Forget B's profile key in A's store before anything starts, so
    # createGroup has to *invite* B and the acceptGroupInvite success path
    # runs on every execution instead of only on first contact.
    if not args.keep_contact:
        ident = subprocess.run([BIN, *args.b_args.split(), "whoami"],
                               capture_output=True, text=True, timeout=60)
        b_uuid = json.loads(ident.stdout)["aci"].replace("-", "").upper()
        db = os.path.expanduser(args.a_db)
        if os.path.exists(db):
            subprocess.run(["sqlite3", db,
                            f"DELETE FROM profile_keys WHERE hex(uuid) = '{b_uuid}'"], check=True)
            print("forgot B's profile key in A's store; expecting the pending-invite path")
        else:
            print(f"warning: {db} not found, cannot forget B")

    A = Node("A", args.a_args.split())
    B = Node("B", args.b_args.split())
    a, b = A.call("whoami"), B.call("whoami")
    a_aci, b_aci = a["aci"], b["aci"]
    print(f"A={a_aci} (device {a['deviceId']})  B={b_aci} (device {b['deviceId']})")
    # Drain both queues so later waits see only fresh traffic.
    for n in (A, B):
        n.wait_event(lambda ev: ev.get("method") == "queueEmpty", timeout=60)

    leave_stray_groups(A)
    leave_stray_groups(B)

    key = None
    try:
        pending = False

        @step("createGroup with B: pending invite on first contact, direct add once A holds B's key")
        def _():
            nonlocal key, pending
            res = A.call("createGroup", title="e2e group", description="two-account run", members=[b_aci])
            key = res["masterKey"]
            g = {g["masterKey"]: g for g in A.call("listGroups")}[key]
            pending = [m["uuid"] for m in g["pendingMembers"]] == [b_aci]
            if pending:
                assert [m["uuid"] for m in g["members"]] == [a_aci], g
            else:
                assert sorted(m["uuid"] for m in g["members"]) == sorted([a_aci, b_aci]), g
            print(f"     (B was {'invited' if pending else 'added directly'})")
        _()

        @step("B receives the creation announcement")
        def _():
            B.wait_event(lambda ev: is_group_message(ev, key), timeout=60)
        _()

        if pending:
            @step("acceptGroupInvite succeeds as an RPC (B)")
            def _():
                B.call("acceptGroupInvite", group=key)
                rev, types = last_change(A, key)
                assert types == ["inviteAccepted"], (rev, types)
                assert rev == 1
            _()
        else:
            print("SKIP acceptGroupInvite success path (A already holds B's profile key; delete B's contact from A's store to re-run it)")

        @step("acceptGroupInvite with no invitation is refused")
        def _():
            try:
                B.call("acceptGroupInvite", group=key)
            except RpcError:
                return
            raise AssertionError("second accept succeeded")
        _()

        @step("profile key exchange via direct messages")
        def _():
            B.call("send", recipient=a_aci, message="hello from B")
            A.wait_event(lambda ev: ev.get("method") == "message" and ev["params"]["envelope"]["sender"] == b_aci, timeout=60)
            A.call("send", recipient=b_aci, message="hello from A")
            B.wait_event(lambda ev: ev.get("method") == "message" and ev["params"]["envelope"]["sender"] == a_aci, timeout=60)
        _()

        @step("attribute mutators from A")
        def _():
            A.call("setGroupTitle", group=key, title="e2e renamed")
            A.call("setGroupDescription", group=key, description="edited")
            A.call("setGroupTimer", group=key, timer=3600)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
                f.write(png(32, 32, (200, 80, 20)))
            A.call("setGroupAvatar", group=key, path=f.name)
            os.unlink(f.name)
            types = [t for _, ts in history_types(B, key)[-4:] for t in ts]
            assert types == ["titleChanged", "descriptionChanged", "timerChanged", "avatarChanged"], types
        _()

        @step("A promotes B; B edits as admin; A demotes B")
        def _():
            A.call("setGroupMemberRole", group=key, member=b_aci, role="ADMINISTRATOR")
            B.call("setGroupTitle", group=key, title="e2e renamed by B")
            B.call("setGroupTimer", group=key, timer=0)
            A.call("setGroupMemberRole", group=key, member=b_aci, role="DEFAULT")
            hist = A.call("getGroupHistory", group=key)["history"][-4:]
            assert [h["editor"] for h in hist] == [a_aci, b_aci, b_aci, a_aci], hist
            assert hist[1]["changes"][0]["title"] == "e2e renamed by B"
        _()

        @step("remove B, re-add B as a full member now that A holds B's key")
        def _():
            A.call("removeGroupMembers", group=key, members=[b_aci])
            assert last_change(A, key)[1] == ["memberRemoved"]
            A.call("addGroupMembers", group=key, members=[b_aci])
            g = {g["masterKey"]: g for g in A.call("listGroups")}[key]
            assert b_aci in [m["uuid"] for m in g["members"]], g
            assert g["pendingMembers"] == [], g
            assert last_change(A, key)[1] == ["memberAdded"]
        _()

        @step("group message from A earns a delivery receipt from B")
        def _():
            # The receipt path is the regression this guards: group messages
            # used to be explicitly excluded from receipting, so senders only
            # ever saw one checkmark.
            sent = A.call("send", group=key, message="receipt check")
            ts = str(sent["timestamp"])
            B.wait_event(lambda ev: is_group_message(ev, key)
                         and ev["params"]["envelope"]["sender"] == a_aci, timeout=60)

            def is_our_receipt(ev):
                if ev.get("method") != "message":
                    return False
                params = ev["params"]
                return (params.get("contentType") == "receiptMessage"
                        and params["envelope"]["sender"] == b_aci
                        and params["content"].get("type") == "DELIVERY"
                        and ts in [str(t) for t in params["content"].get("timestamp", [])])

            A.wait_event(is_our_receipt, timeout=60)
        _()

        @step("re-adding an existing member is a no-op (no revision bump)")
        def _():
            before = last_change(A, key)[0]
            A.call("addGroupMembers", group=key, members=[b_aci])
            assert last_change(A, key)[0] == before
        _()

        link = None

        @step("invite link (anyone): B previews and joins directly")
        def _():
            nonlocal link
            A.call("removeGroupMembers", group=key, members=[b_aci])
            link = A.call("setGroupInviteLink", group=key, access="anyone")["inviteLink"]
            assert link and link.startswith("https://signal.group/#"), link
            assert A.call("getGroupInviteLink", group=key)["inviteLink"] == link
            info = B.call("getGroupJoinInfo", link=link)
            assert info["title"] == "e2e renamed by B", info
            assert info["requiresAdminApproval"] is False and info["linkAccess"] == "ANY", info
            assert info["requestPending"] is False, info
            res = B.call("joinGroup", link=link)
            assert res["status"] == "joined", res
            assert last_change(A, key)[1] == ["memberAdded"]
            assert b_aci in [m["uuid"] for m in {g["masterKey"]: g for g in B.call("listGroups")}[key]["members"]]
        _()

        @step("invite link (adminApproval): request → deny → request → approve")
        def _():
            A.call("removeGroupMembers", group=key, members=[b_aci])
            res = A.call("setGroupInviteLink", group=key, access="adminApproval")
            assert res["inviteLink"] == link, "password must be kept when only access changes"
            info = B.call("getGroupJoinInfo", link=link)
            assert info["requiresAdminApproval"] is True and info["linkAccess"] == "ADMINISTRATOR", info
            assert info["requestPending"] is False, info
            assert B.call("joinGroup", link=link)["status"] == "requested"
            assert last_change(A, key)[1] == ["joinRequested"]
            assert B.call("getGroupJoinInfo", link=link)["requestPending"] is True
            A.call("denyGroupJoinRequests", group=key, members=[b_aci])
            assert last_change(A, key)[1] == ["joinRequestDenied"]
            assert B.call("joinGroup", link=link)["status"] == "requested"
            A.call("approveGroupJoinRequests", group=key, members=[b_aci])
            assert last_change(A, key)[1] == ["joinRequestApproved"]
            assert b_aci in [m["uuid"] for m in {g["masterKey"]: g for g in A.call("listGroups")}[key]["members"]]
        _()

        @step("ban blocks the invite link; unban restores it")
        def _():
            A.call("removeGroupMembers", group=key, members=[b_aci])
            A.call("banGroupMembers", group=key, members=[b_aci])
            assert last_change(A, key)[1] == ["memberBanned"]
            banned = A.call("getGroup", group=key)["bannedMembers"]
            assert [b["serviceId"] for b in banned] == [b_aci], banned
            for method, kw in (("joinGroup", {"link": link}), ("getGroupJoinInfo", {"link": link})):
                try:
                    B.call(method, **kw)
                    raise AssertionError(f"{method} succeeded while banned")
                except RpcError as e:
                    assert "no longer valid" in str(e), (method, e)
            A.call("unbanGroupMembers", group=key, members=[b_aci])
            assert last_change(A, key)[1] == ["memberUnbanned"]
            assert A.call("getGroup", group=key)["bannedMembers"] == []
            assert B.call("joinGroup", link=link)["status"] == "requested"
            A.call("approveGroupJoinRequests", group=key, members=[b_aci])
        _()

        @step("attributes access ADMINISTRATOR blocks a plain member server-side")
        def _():
            A.call("setGroupAttributesAccess", group=key, access="ADMINISTRATOR")
            assert last_change(A, key)[1] == ["attributeAccessChanged"]
            try:
                B.call("setGroupTitle", group=key, title="B should not be able to do this")
                raise AssertionError("non-admin title change succeeded")
            except RpcError:
                pass
            A.call("setGroupAttributesAccess", group=key, access="MEMBER")
            B.call("setGroupTitle", group=key, title="e2e renamed by B")
        _()

        @step("members access ADMINISTRATOR blocks a plain member server-side")
        def _():
            A.call("setGroupMembersAccess", group=key, access="ADMINISTRATOR")
            assert last_change(A, key)[1] == ["memberAccessChanged"]
            bogus = "00000000-0000-4000-8000-000000000042"
            try:
                B.call("addGroupMembers", group=key, members=[bogus])
                raise AssertionError("non-admin member add succeeded")
            except RpcError:
                pass
            finally:
                # If the assert above ever fails, don't leave a bogus invite.
                A.call("removeGroupMembers", group=key, members=[bogus])
            A.call("setGroupMembersAccess", group=key, access="MEMBER")
        _()

        @step("announcements-only is admin-gated and toggles")
        def _():
            try:
                B.call("setGroupAnnouncementsOnly", group=key, enabled=True)
                raise AssertionError("non-admin announcements toggle succeeded")
            except RpcError:
                pass
            A.call("setGroupAnnouncementsOnly", group=key, enabled=True)
            assert last_change(A, key)[1] == ["announcementsOnlyChanged"]
            assert A.call("getGroup", group=key)["announcementsOnly"] is True
            before = last_change(A, key)[0]
            A.call("setGroupAnnouncementsOnly", group=key, enabled=True)
            assert last_change(A, key)[0] == before, "no-op toggle bumped revision"
            A.call("setGroupAnnouncementsOnly", group=key, enabled=False)
            assert A.call("getGroup", group=key)["announcementsOnly"] is False
        _()

        @step("approving a non-requester is a no-op")
        def _():
            before = last_change(A, key)[0]
            A.call("approveGroupJoinRequests", group=key, members=[b_aci])
            assert last_change(A, key)[0] == before
        _()

        @step("resetPassword rotates the link; disabling revokes it")
        def _():
            link2 = A.call("setGroupInviteLink", group=key, access="anyone", resetPassword=True)["inviteLink"]
            assert link2 != link
            try:
                B.call("getGroupJoinInfo", link=link)
                raise AssertionError("old link still valid after password reset")
            except RpcError as e:
                assert "no longer valid" in str(e), e
            assert A.call("setGroupInviteLink", group=key, access="disabled")["inviteLink"] is None
            try:
                B.call("getGroupJoinInfo", link=link2)
                raise AssertionError("disabled link still valid")
            except RpcError as e:
                assert "no longer valid" in str(e), e
            assert A.call("getGroupInviteLink", group=key)["inviteLink"] is None
        _()

        @step("PNI invite: B accepts via PNI promotion")
        def _():
            b_pni = b["pni"]
            res = A.call("createGroup", title="e2e pni", members=[f"PNI:{b_pni}"])
            key2 = res["masterKey"]
            try:
                g = A.call("getGroup", group=key2)
                assert [m["serviceId"] for m in g["pendingMembers"]] == [f"PNI:{b_pni}"], g
                B.call("acceptGroupInvite", group=key2)
                g = A.call("getGroup", group=key2)
                assert b_aci in [m["uuid"] for m in g["members"]], g
                assert g["pendingMembers"] == [], g
                assert last_change(A, key2)[1] == ["inviteAcceptedByPni"]
            finally:
                for n in (B, A):
                    try:
                        n.call("leaveGroup", group=key2, force=True)
                    except RpcError:
                        pass
        _()

        @step("history is consistent between accounts")
        def _():
            ha = [(e["revision"], e["editor"], [c["type"] for c in e["changes"]]) for e in A.call("getGroupHistory", group=key)["history"]]
            hb = [(e["revision"], e["editor"], [c["type"] for c in e["changes"]]) for e in B.call("getGroupHistory", group=key)["history"]]
            # B can only see history from its most recent join; that suffix must match A's.
            assert hb == ha[-len(hb):], (ha, hb)
            assert len(ha) >= 20, len(ha)
        _()

    finally:
        if key:
            @step("both accounts leave")
            def _():
                errors = []
                for n in (B, A):
                    try:
                        n.call("leaveGroup", group=key, force=True)
                    except RpcError as e:
                        errors.append(f"{n.name}: {e}")
                # B may legitimately not be a member if a step failed early.
                assert not [e for e in errors if e.startswith("A")], errors
            try:
                _()
            except Exception:
                pass
        leave_stray_groups(A)
        A.close(); B.close()

    print(f"\n{len(passed)} passed, {len(failed)} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
