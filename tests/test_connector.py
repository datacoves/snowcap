"""Unit tests for snowcap/operations/connector.py

Tests the connection setup and authentication functionality.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

import pytest
from click.exceptions import ClickException
from snowflake.connector.errors import DatabaseError, ForbiddenError

from snowcap.operations.connector import (
    connect,
    get_env_vars,
    _load_pem_to_der,
    _update_connection_details_with_private_key,
    _raise_errors_related_to_session_token,
    _avoid_closing_the_connection_if_it_was_shared,
    _update_connection_application_name,
    InvalidConnectionConfiguration,
    SnowflakeConnectionError,
    DirectoryIsNotEmptyError,
    FileTooLargeError,
    SecurePath,
    ENCRYPTED_PKCS8_PK_HEADER,
    UNENCRYPTED_PKCS8_PK_HEADER,
    DEFAULT_SIZE_LIMIT_MB,
    UNLIMITED,
)


# =============================================================================
# Test Exception Classes
# =============================================================================


class TestInvalidConnectionConfiguration:
    """Tests for InvalidConnectionConfiguration exception."""

    def test_format_message(self):
        exc = InvalidConnectionConfiguration("test message")
        assert exc.format_message() == "Invalid connection configuration. test message"

    def test_empty_message(self):
        exc = InvalidConnectionConfiguration("")
        assert exc.format_message() == "Invalid connection configuration. "


class TestSnowflakeConnectionError:
    """Tests for SnowflakeConnectionError exception."""

    def test_error_message(self):
        original_error = Exception("Original error message")
        exc = SnowflakeConnectionError(original_error)
        assert "Could not connect to Snowflake" in str(exc.message)
        assert "Original error message" in str(exc.message)


class TestDirectoryIsNotEmptyError:
    """Tests for DirectoryIsNotEmptyError exception."""

    def test_error_message(self):
        exc = DirectoryIsNotEmptyError(Path("/test/path"))
        assert "Directory '/test/path' is not empty" in str(exc.message)


class TestFileTooLargeError:
    """Tests for FileTooLargeError exception."""

    def test_error_message(self):
        exc = FileTooLargeError(Path("/test/file.txt"), 128)
        assert "File /test/file.txt is too large" in str(exc.message)
        assert "128 KB" in str(exc.message)


# =============================================================================
# Test get_env_vars()
# =============================================================================


class TestGetEnvVars:
    """Tests for get_env_vars() function."""

    def test_returns_empty_dict_when_no_vars_set(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_env_vars()
            assert result == {}

    def test_returns_account_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "myaccount"}, clear=True):
            result = get_env_vars()
            assert result == {"account": "myaccount"}

    def test_returns_user_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_USER": "myuser"}, clear=True):
            result = get_env_vars()
            assert result == {"user": "myuser"}

    def test_returns_password_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_PASSWORD": "mypassword"}, clear=True):
            result = get_env_vars()
            assert result == {"password": "mypassword"}

    def test_returns_multiple_vars(self):
        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
            "SNOWFLAKE_DATABASE": "db",
            "SNOWFLAKE_SCHEMA": "schema",
            "SNOWFLAKE_ROLE": "role",
            "SNOWFLAKE_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, env, clear=True):
            result = get_env_vars()
            assert result == {
                "account": "acct",
                "user": "user",
                "password": "pass",
                "database": "db",
                "schema": "schema",
                "role": "role",
                "warehouse": "wh",
            }

    def test_returns_mfa_passcode_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_MFA_PASSCODE": "123456"}, clear=True):
            result = get_env_vars()
            assert result == {"mfa_passcode": "123456"}

    def test_returns_authenticator_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_AUTHENTICATOR": "SNOWFLAKE_JWT"}, clear=True):
            result = get_env_vars()
            assert result == {"authenticator": "SNOWFLAKE_JWT"}

    def test_returns_private_key_path_when_set(self):
        with patch.dict(os.environ, {"SNOWFLAKE_PRIVATE_KEY_PATH": "/path/to/key.pem"}, clear=True):
            result = get_env_vars()
            assert result == {"private_key_path": "/path/to/key.pem"}

    def test_ignores_empty_values(self):
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": ""}, clear=True):
            result = get_env_vars()
            assert result == {}


# =============================================================================
# Test _raise_errors_related_to_session_token()
# =============================================================================


class TestRaiseErrorsRelatedToSessionToken:
    """Tests for _raise_errors_related_to_session_token() function."""

    def test_no_error_when_neither_token_provided(self):
        # Should not raise
        _raise_errors_related_to_session_token(False, False)

    def test_no_error_when_both_tokens_provided(self):
        # Should not raise
        _raise_errors_related_to_session_token(True, True)

    def test_raises_when_session_token_without_master(self):
        with pytest.raises(ClickException) as exc_info:
            _raise_errors_related_to_session_token(True, False)
        assert "master token" in str(exc_info.value.message)

    def test_raises_when_master_token_without_session(self):
        with pytest.raises(ClickException) as exc_info:
            _raise_errors_related_to_session_token(False, True)
        assert "session token" in str(exc_info.value.message)


# =============================================================================
# Test _avoid_closing_the_connection_if_it_was_shared()
# =============================================================================


class TestAvoidClosingConnectionIfShared:
    """Tests for _avoid_closing_the_connection_if_it_was_shared() function."""

    def test_sets_keep_alive_when_both_tokens(self):
        params = {}
        _avoid_closing_the_connection_if_it_was_shared(True, True, params)
        assert params["server_session_keep_alive"] is True

    def test_does_not_set_keep_alive_when_no_session_token(self):
        params = {}
        _avoid_closing_the_connection_if_it_was_shared(False, True, params)
        assert "server_session_keep_alive" not in params

    def test_does_not_set_keep_alive_when_no_master_token(self):
        params = {}
        _avoid_closing_the_connection_if_it_was_shared(True, False, params)
        assert "server_session_keep_alive" not in params

    def test_does_not_set_keep_alive_when_neither_token(self):
        params = {}
        _avoid_closing_the_connection_if_it_was_shared(False, False, params)
        assert "server_session_keep_alive" not in params


# =============================================================================
# Test _update_connection_application_name()
# =============================================================================


class TestUpdateConnectionApplicationName:
    """Tests for _update_connection_application_name() function."""

    def test_sets_application_name(self):
        params = {}
        _update_connection_application_name(params)
        assert params["application_name"] == "snowcap"

    def test_overwrites_existing_application_name(self):
        params = {"application_name": "other"}
        _update_connection_application_name(params)
        assert params["application_name"] == "snowcap"


# =============================================================================
# Test _update_connection_details_with_private_key()
# =============================================================================


class TestUpdateConnectionDetailsWithPrivateKey:
    """Tests for _update_connection_details_with_private_key() function."""

    def test_returns_params_unchanged_without_private_key_path(self):
        params = {"account": "test", "user": "testuser"}
        result = _update_connection_details_with_private_key(params)
        assert result == {"account": "test", "user": "testuser"}

    def test_raises_when_private_key_without_jwt_authenticator(self):
        params = {"private_key_path": "/path/to/key.pem", "authenticator": "password"}
        with pytest.raises(ClickException) as exc_info:
            _update_connection_details_with_private_key(params)
        assert "SNOWFLAKE_JWT" in str(exc_info.value.message)

    def test_raises_when_private_key_without_authenticator(self):
        params = {"private_key_path": "/path/to/key.pem"}
        with pytest.raises(ClickException) as exc_info:
            _update_connection_details_with_private_key(params)
        assert "SNOWFLAKE_JWT" in str(exc_info.value.message)

    @patch("snowcap.operations.connector._load_pem_to_der")
    def test_converts_key_path_to_private_key(self, mock_load_pem):
        mock_load_pem.return_value = b"der_key_bytes"
        params = {"private_key_path": "/path/to/key.pem", "authenticator": "SNOWFLAKE_JWT"}
        result = _update_connection_details_with_private_key(params)
        assert "private_key" in result
        assert result["private_key"] == b"der_key_bytes"
        assert "private_key_path" not in result
        mock_load_pem.assert_called_once_with("/path/to/key.pem")


# =============================================================================
# Test _load_pem_to_der()
# =============================================================================


class TestLoadPemToDer:
    """Tests for _load_pem_to_der() function."""

    def test_loads_unencrypted_private_key(self):
        # Generate a test unencrypted private key
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
        )

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem_data = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as f:
            f.write(pem_data)
            key_path = f.name

        try:
            with patch.dict(os.environ, {}, clear=True):
                result = _load_pem_to_der(key_path)
                assert isinstance(result, bytes)
                assert len(result) > 0
        finally:
            os.unlink(key_path)

    def test_raises_for_encrypted_key_without_passphrase(self):
        # Generate a test encrypted private key
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            BestAvailableEncryption,
            PrivateFormat,
        )

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem_data = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=BestAvailableEncryption(b"testpassword"),
        )

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as f:
            f.write(pem_data)
            key_path = f.name

        try:
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(ClickException) as exc_info:
                    _load_pem_to_der(key_path)
                assert "passphrase" in str(exc_info.value.message).lower()
        finally:
            os.unlink(key_path)

    def test_loads_encrypted_key_with_passphrase(self):
        # Generate a test encrypted private key
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            BestAvailableEncryption,
            PrivateFormat,
        )

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem_data = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=BestAvailableEncryption(b"testpassword"),
        )

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as f:
            f.write(pem_data)
            key_path = f.name

        try:
            with patch.dict(os.environ, {"PRIVATE_KEY_PASSPHRASE": "testpassword"}, clear=True):
                result = _load_pem_to_der(key_path)
                assert isinstance(result, bytes)
                assert len(result) > 0
        finally:
            os.unlink(key_path)

    def test_raises_for_invalid_key_format(self):
        # Create a file with invalid key format
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as f:
            f.write(b"-----BEGIN RSA PRIVATE KEY-----\nInvalidKeyData\n-----END RSA PRIVATE KEY-----")
            key_path = f.name

        try:
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(ClickException) as exc_info:
                    _load_pem_to_der(key_path)
                assert "PKCS#8" in str(exc_info.value.message)
        finally:
            os.unlink(key_path)


# =============================================================================
# Test connect()
# =============================================================================


class TestConnect:
    """Tests for connect() function."""

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_with_password(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(account="test", user="testuser", password="testpass")

        assert result == mock_conn
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["account"] == "test"
        assert call_kwargs["user"] == "testuser"
        assert call_kwargs["password"] == "testpass"
        assert call_kwargs["application_name"] == "snowcap"

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_uses_env_vars(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "envaccount", "user": "envuser"}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect()

        assert result == mock_conn
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["account"] == "envaccount"
        assert call_kwargs["user"] == "envuser"

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_kwargs_override_env(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "envaccount", "user": "envuser"}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(account="override")

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["account"] == "override"

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_with_mfa_passcode(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "test", "user": "testuser"}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(mfa_passcode="123456")

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["passcode"] == "123456"

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_with_username_password_mfa_authenticator(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(account="test", user="user", authenticator="username_password_mfa")

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["client_request_mfa_token"] is True

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_with_diagnostics_enabled(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "test", "user": "user"}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(enable_diag=True, diag_log_path="/tmp/log", diag_allowlist_path="/tmp/allowlist")

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["enable_connection_diag"] is True
        assert call_kwargs["connection_diag_log_path"] == "/tmp/log"
        assert call_kwargs["connection_diag_allowlist_path"] == "/tmp/allowlist"

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_with_session_and_master_token(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(
            account="test", user="user", session_token="session123", master_token="master123"
        )

        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["server_session_keep_alive"] is True

    def test_connect_raises_for_session_token_without_master(self):
        with pytest.raises(ClickException) as exc_info:
            connect(account="test", user="user", session_token="session123")
        assert "master token" in str(exc_info.value.message)

    def test_connect_raises_for_master_token_without_session(self):
        with pytest.raises(ClickException) as exc_info:
            connect(account="test", user="user", master_token="master123")
        assert "session token" in str(exc_info.value.message)

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_raises_connection_error_on_forbidden(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "test", "user": "user"}
        # ForbiddenError requires no positional arguments
        forbidden_error = ForbiddenError()
        forbidden_error.msg = "Access denied"
        mock_connect.side_effect = forbidden_error

        with pytest.raises(SnowflakeConnectionError):
            connect()

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_raises_invalid_config_on_database_error(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "test", "user": "user"}
        db_error = DatabaseError("Invalid credentials")
        db_error.msg = "Invalid credentials"
        mock_connect.side_effect = db_error

        with pytest.raises(InvalidConnectionConfiguration):
            connect()

    @patch("snowcap.operations.connector.snowflake.connector.connect")
    @patch("snowcap.operations.connector.get_env_vars")
    def test_connect_filters_none_values(self, mock_get_env, mock_connect):
        mock_get_env.return_value = {"account": "test"}
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect(user=None, password=None)

        call_kwargs = mock_connect.call_args[1]
        assert "user" not in call_kwargs
        assert "password" not in call_kwargs


# =============================================================================
# Test SecurePath
# =============================================================================


class TestSecurePath:
    """Tests for SecurePath class."""

    def test_init_with_path(self):
        sp = SecurePath(Path("/test/path"))
        assert sp.path == Path("/test/path")

    def test_init_with_string(self):
        sp = SecurePath("/test/path")
        assert sp.path == Path("/test/path")

    def test_repr(self):
        sp = SecurePath("/test/path")
        assert repr(sp) == 'SecurePath("/test/path")'

    def test_truediv(self):
        sp = SecurePath("/test")
        result = sp / "subdir"
        assert isinstance(result, SecurePath)
        assert result.path == Path("/test/subdir")

    def test_parent(self):
        sp = SecurePath("/test/path/file.txt")
        assert isinstance(sp.parent, SecurePath)
        assert sp.parent.path == Path("/test/path")

    def test_absolute(self):
        sp = SecurePath("relative/path")
        result = sp.absolute()
        assert isinstance(result, SecurePath)
        assert result.path.is_absolute()

    def test_exists(self, tmp_path):
        existing = tmp_path / "existing.txt"
        existing.touch()
        sp_exists = SecurePath(existing)
        sp_not_exists = SecurePath(tmp_path / "not_existing.txt")

        assert sp_exists.exists() is True
        assert sp_not_exists.exists() is False

    def test_is_dir(self, tmp_path):
        sp_dir = SecurePath(tmp_path)
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sp_file = SecurePath(file_path)

        assert sp_dir.is_dir() is True
        assert sp_file.is_dir() is False

    def test_is_file(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sp_file = SecurePath(file_path)
        sp_dir = SecurePath(tmp_path)

        assert sp_file.is_file() is True
        assert sp_dir.is_file() is False

    def test_name(self):
        sp = SecurePath("/test/path/file.txt")
        assert sp.name == "file.txt"

    def test_touch_creates_file(self, tmp_path):
        file_path = tmp_path / "new_file.txt"
        sp = SecurePath(file_path)
        sp.touch()
        assert file_path.exists()

    def test_mkdir_creates_directory(self, tmp_path):
        dir_path = tmp_path / "new_dir"
        sp = SecurePath(dir_path)
        sp.mkdir()
        assert dir_path.is_dir()

    def test_mkdir_with_parents(self, tmp_path):
        nested_path = tmp_path / "parent" / "child" / "grandchild"
        sp = SecurePath(nested_path)
        sp.mkdir(parents=True)
        assert nested_path.is_dir()

    def test_read_text(self, tmp_path):
        file_path = tmp_path / "test.txt"
        file_path.write_text("Hello, World!")
        sp = SecurePath(file_path)

        result = sp.read_text(file_size_limit_mb=1)
        assert result == "Hello, World!"

    def test_read_text_raises_for_large_file(self, tmp_path):
        file_path = tmp_path / "large.txt"
        # Create a file larger than 1 byte (using size_limit_mb=0 would cause division by zero)
        file_path.write_bytes(b"x" * (2 * 1024 * 1024))  # 2 MB
        sp = SecurePath(file_path)

        with pytest.raises(FileTooLargeError):
            sp.read_text(file_size_limit_mb=1)

    def test_write_text(self, tmp_path):
        file_path = tmp_path / "output.txt"
        sp = SecurePath(file_path)
        sp.write_text("Test content")
        assert file_path.read_text() == "Test content"

    def test_iterdir(self, tmp_path):
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.txt").touch()
        sp = SecurePath(tmp_path)

        items = list(sp.iterdir())
        assert len(items) == 2
        assert all(isinstance(item, SecurePath) for item in items)

    def test_iterdir_raises_for_file(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sp = SecurePath(file_path)

        with pytest.raises(NotADirectoryError):
            list(sp.iterdir())

    def test_unlink(self, tmp_path):
        file_path = tmp_path / "to_delete.txt"
        file_path.touch()
        sp = SecurePath(file_path)

        sp.unlink()
        assert not file_path.exists()

    def test_unlink_missing_ok(self, tmp_path):
        sp = SecurePath(tmp_path / "nonexistent.txt")
        sp.unlink(missing_ok=True)  # Should not raise

    def test_unlink_raises_for_missing(self, tmp_path):
        sp = SecurePath(tmp_path / "nonexistent.txt")
        with pytest.raises(FileNotFoundError):
            sp.unlink()

    def test_rmdir(self, tmp_path):
        dir_path = tmp_path / "to_delete"
        dir_path.mkdir()
        sp = SecurePath(dir_path)

        sp.rmdir()
        assert not dir_path.exists()

    def test_rmdir_recursive(self, tmp_path):
        dir_path = tmp_path / "to_delete"
        dir_path.mkdir()
        (dir_path / "file.txt").touch()
        sp = SecurePath(dir_path)

        sp.rmdir(recursive=True)
        assert not dir_path.exists()

    def test_rmdir_raises_for_non_empty(self, tmp_path):
        dir_path = tmp_path / "non_empty"
        dir_path.mkdir()
        (dir_path / "file.txt").touch()
        sp = SecurePath(dir_path)

        with pytest.raises(DirectoryIsNotEmptyError):
            sp.rmdir()

    def test_copy_file(self, tmp_path):
        src = tmp_path / "source.txt"
        src.write_text("content")
        dst = tmp_path / "dest.txt"

        sp = SecurePath(src)
        result = sp.copy(dst)

        assert isinstance(result, SecurePath)
        assert dst.read_text() == "content"

    def test_copy_directory(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "file.txt").write_text("content")
        dst_dir = tmp_path / "dst"

        sp = SecurePath(src_dir)
        result = sp.copy(dst_dir)

        assert dst_dir.is_dir()
        assert (dst_dir / "file.txt").read_text() == "content"

    def test_move_file(self, tmp_path):
        src = tmp_path / "source.txt"
        src.write_text("content")
        dst = tmp_path / "dest.txt"

        sp = SecurePath(src)
        result = sp.move(dst)

        assert isinstance(result, SecurePath)
        assert dst.read_text() == "content"
        assert not src.exists()

    def test_temporary_directory(self):
        with SecurePath.temporary_directory() as tmpdir:
            assert isinstance(tmpdir, SecurePath)
            assert tmpdir.exists()
            assert tmpdir.is_dir()

    def test_assert_exists_raises(self, tmp_path):
        sp = SecurePath(tmp_path / "nonexistent")
        with pytest.raises(FileNotFoundError):
            sp.assert_exists()

    def test_assert_is_file_raises_for_directory(self, tmp_path):
        sp = SecurePath(tmp_path)
        with pytest.raises(IsADirectoryError):
            sp.assert_is_file()

    def test_assert_is_directory_raises_for_file(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sp = SecurePath(file_path)
        with pytest.raises(NotADirectoryError):
            sp.assert_is_directory()

    def test_open_for_reading(self, tmp_path):
        file_path = tmp_path / "test.txt"
        file_path.write_text("test content")
        sp = SecurePath(file_path)

        with sp.open("r", read_file_limit_mb=1) as f:
            content = f.read()
        assert content == "test content"

    def test_open_for_writing(self, tmp_path):
        file_path = tmp_path / "output.txt"
        sp = SecurePath(file_path)

        with sp.open("w") as f:
            f.write("written content")
        assert file_path.read_text() == "written content"

    def test_chmod(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sp = SecurePath(file_path)

        sp.chmod(0o600)
        mode = file_path.stat().st_mode & 0o777
        assert mode == 0o600


# =============================================================================
# Test Constants
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_default_size_limit(self):
        assert DEFAULT_SIZE_LIMIT_MB == 128

    def test_unlimited(self):
        assert UNLIMITED == -1

    def test_encrypted_header(self):
        assert ENCRYPTED_PKCS8_PK_HEADER == b"-----BEGIN ENCRYPTED PRIVATE KEY-----"

    def test_unencrypted_header(self):
        assert UNENCRYPTED_PKCS8_PK_HEADER == b"-----BEGIN PRIVATE KEY-----"
