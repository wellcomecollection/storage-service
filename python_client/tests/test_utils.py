# -*- encoding: utf-8

import os

import mock
import pytest

from wellcome_storage_service import _utils as utils


class TestMkdirP:
    # This is based on the tests for a similar function in loris-imageserver/loris:
    # https://github.com/loris-imageserver/loris/blob/5daf001fe9bcbc1acfae85226c70d99a7f77b948/tests/utils_t.py

    def test_creates_directory(self, tmpdir):
        path = str(tmpdir.join("test_creates_directory"))
        assert not os.path.exists(path)

        # If we create the directory, it springs into being
        utils.mkdir_p(path)
        assert os.path.exists(path)

        # If we try to create the directory a second time, we don't throw
        # an exception just because it already exists.
        utils.mkdir_p(path)

    def test_if_error_is_unexpected_then_is_raised(self, tmpdir):
        """
        If the error from ``os.makedirs()`` isn't because the directory
        already exists, we get an error.
        """
        path = str(tmpdir.join("test_if_error_is_unexpected_then_is_raised"))

        message = "Exception thrown in test_utils.py for TestMkdirP"

        m = mock.Mock(side_effect=OSError(-1, message))
        with mock.patch("wellcome_storage_service._utils.os.makedirs", m):
            with pytest.raises(OSError, match=message):
                utils.mkdir_p(path)
