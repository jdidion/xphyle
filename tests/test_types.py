from unittest import TestCase
from xphyle.types import *


class TypeTests(TestCase):
    def test_mode_access(self):
        for t in ("READ", "READWRITE", "TRUNCATE_READWRITE"):
            self.assertTrue(ModeAccess[t].readable)
        for t in ("WRITE", "READWRITE", "TRUNCATE_READWRITE", "APPEND", "EXCLUSIVE"):
            self.assertTrue(ModeAccess[t].writable)

    def test_file_mode(self):
        for f in (
            FileMode(),
            FileMode("rt"),
            FileMode(access="r"),
            FileMode(coding="t"),
            FileMode(access=ModeAccess.READ),
            FileMode(coding=ModeCoding.TEXT),
            FileMode(access="r", coding="t"),
            FileMode(access=ModeAccess.READ, coding="t"),
            FileMode(access="r", coding=ModeCoding.TEXT),
            FileMode(access=ModeAccess.READ, coding=ModeCoding.TEXT),
        ):
            self.assertEqual(ModeAccess.READ, f.access)
            self.assertEqual(ModeCoding.TEXT, f.coding)
            self.assertTrue(f.readable)
            self.assertFalse(f.writable)
            self.assertTrue(f.text)
            self.assertFalse(f.binary)
            self.assertTrue("rt" in f)
            self.assertFalse("b" in f)
            self.assertTrue(ModeAccess.READ in f)
            self.assertTrue(ModeCoding.TEXT in f)
            self.assertEqual("rt", f.value)
            self.assertEqual("rt", str(f))
        with self.assertRaises(ValueError):
            FileMode("rz")

    def test_permissions(self):
        self.assertEqual(os.R_OK, Permission.READ.os_flag)
        self.assertEqual(os.W_OK, Permission.WRITE.os_flag)
        self.assertEqual(os.X_OK, Permission.EXECUTE.os_flag)
        self.assertEqual(stat.S_IREAD, Permission.READ.stat_flag)
        self.assertEqual(stat.S_IWRITE, Permission.WRITE.stat_flag)
        self.assertEqual(stat.S_IEXEC, Permission.EXECUTE.stat_flag)

    def test_permission_set(self):
        for a in (
            PermissionSet("rwx"),
            PermissionSet(("r", "w", "x")),
            PermissionSet(7),
            PermissionSet((1, 2, 4)),
            PermissionSet((Permission.READ, Permission.WRITE, Permission.EXECUTE)),
        ):
            self.assertEqual(7, a.os_flags)
            self.assertEqual(448, a.stat_flags)
            self.assertEqual("rwx", "".join(f.value for f in a))
            self.assertEqual("rwx", str(a))
            for char in "rwx":
                self.assertTrue(char in a)
                self.assertTrue(Permission(char) in a)

        a = PermissionSet()
        a.add(ModeAccess.READ)
        a.add(ModeAccess.WRITE)
        self.assertEqual("rw", str(a))

    def test_cache(self):
        fm1 = FileMode("rt")
        fm2 = FileMode("rt")
        fm3 = FileMode("tr")
        self.assertEqual(fm1, fm2)
        self.assertEqual(fm1, fm3)
        self.assertEqual(id(fm1), id(fm2))
        self.assertNotEqual(id(fm1), id(fm3))

        perm1 = PermissionSet("rw")
        perm2 = PermissionSet("rw")
        perm3 = PermissionSet("wr")
        self.assertEqual(perm1, perm2)
        self.assertEqual(perm1, perm3)
        self.assertEqual(id(perm1), id(perm2))
        self.assertNotEqual(id(perm1), id(perm3))
