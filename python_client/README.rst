wellcome-storage-service
========================

This is a client for the `Wellcome Storage Service <https://github.com/wellcometrust/storage-service>`_.

Uploading a new version
***********************

To upload a new version:

1. Bump the version in ``src/wellcome_storage_service/version.py``.

2. Add a changelog entry in ``CHANGELOG.md``.

3. Create a new package for PyPI by running::

      cd /repos/storage-service/python_client
      python setup.py sdist

4. Upload the new package to PyPI by running::

      twine upload dist/wellcome_storage_service-<version>.tar.gz

5. Enter the PyPI username (``wellcomedigitalplatform``) and password (see Keybase).

6. Celebrate your new release. 🎉✨
