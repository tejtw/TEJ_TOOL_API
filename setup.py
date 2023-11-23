# coding=UTF-8
from setuptools import  setup  # noqa: E402


def myversion():
  def my_release_branch_semver_version(version):
      return str(version.tag)
  return {
      'version_scheme': my_release_branch_semver_version,
      'local_scheme': 'no-local-version',
  }


setup(
    use_scm_version=myversion,
)
