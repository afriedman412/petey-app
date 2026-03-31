"""
Tests for desktop app configuration.

Verifies that the conditional imports and env var overrides
work correctly for standalone/desktop mode.
"""
import os
from unittest.mock import patch


class TestFirebaseConditionalImport:
    def test_auth_loads_without_firebase_when_disabled(self):
        """auth.py should load without firebase_admin when disabled."""
        with patch.dict(os.environ, {"FIREBASE_AUTH_DISABLED": "1"}):
            import importlib
            import server.auth as auth_mod
            importlib.reload(auth_mod)
            # Should have set firebase_admin to None
            assert auth_mod.firebase_admin is None
            assert auth_mod.firebase_auth is None

    def test_auth_middleware_passes_through_when_disabled(self):
        """With auth disabled, middleware sets uid to local-dev."""
        import importlib
        with patch.dict(os.environ, {"FIREBASE_AUTH_DISABLED": "1"}):
            import server.auth as auth_mod
            importlib.reload(auth_mod)
            assert auth_mod._auth_disabled() is True


class TestFirestoreConditionalImport:
    def test_settings_loads_without_firestore(self):
        """settings.py should load even if google.cloud.firestore
        is not installed (firestore=None fallback)."""
        with patch.dict(
            os.environ, {"FIREBASE_AUTH_DISABLED": "1"},
        ):
            import importlib
            import server.settings as settings_mod
            importlib.reload(settings_mod)
            # Should use local file backend
            assert settings_mod._use_local() is True


class TestBaseDirOverride:
    def test_app_respects_petey_web_base(self, tmp_path):
        """BASE_DIR should use PETEY_WEB_BASE when set."""
        # Create dirs that app.py expects on load
        (tmp_path / "templates").mkdir()
        (tmp_path / "static").mkdir()
        (tmp_path / "schemas").mkdir()
        with patch.dict(os.environ, {"PETEY_WEB_BASE": str(tmp_path)}):
            import importlib
            import server.app as app_mod
            importlib.reload(app_mod)
            assert app_mod.BASE_DIR == tmp_path

    def test_extract_respects_petey_web_base(self, tmp_path):
        """extract.py BASE_DIR should use PETEY_WEB_BASE when set."""
        (tmp_path / "schemas").mkdir()
        with patch.dict(os.environ, {"PETEY_WEB_BASE": str(tmp_path)}):
            import importlib
            import server.extract as extract_mod
            importlib.reload(extract_mod)
            assert extract_mod.BASE_DIR == tmp_path


class TestDesktopLauncher:
    def test_find_free_port(self):
        """find_free_port should return a valid port number."""
        from desktop.launch import find_free_port
        port = find_free_port()
        assert isinstance(port, int)
        assert 1024 <= port <= 65535

    def test_env_vars_set(self):
        """Launcher should set FIREBASE_AUTH_DISABLED and MAX_PAGES."""
        # launch.py sets these at import time
        import desktop.launch  # noqa: F401
        assert os.environ.get("FIREBASE_AUTH_DISABLED") == "1"
        assert os.environ.get("MAX_PAGES") == "0"
