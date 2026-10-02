from setuptools import setup, find_namespace_packages


with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as fh:
    version = fh.read().strip()


setup(
    name='cltl.emissor-data',
    version=version,
    package_dir={'': 'src'},
    packages=find_namespace_packages(include=['cltl.*', 'cltl_service.*'], where='src'),
    data_files=[('VERSION', ['VERSION'])],
    url="https://github.com/leolani/cltl-emissor-data",
    license='MIT License',
    author='CLTL',
    author_email='t.baier@vu.nl',
    description='Template component for Leolani',
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.8',
    install_requires=['cltl.combot'],
    extras_require={
        # cv2 is deliberately NOT declared, the same way cltl.chat-ui does not
        # declare it: two differently named distributions provide it --
        # `opencv-python` in the application and harness virtual environments,
        # `opencv-python-headless` in cltl-base-slim -- and naming either one
        # breaks the other environment's `--no-index` install. This extra named
        # `opencv-python`, which is what kept this component off the slim base.
        #
        # `cltl.emissordata.file_storage` imports it inside a `try` and degrades to
        # `image_loader = None`, so the dependency is genuinely optional. Note the
        # guard there reads `except ImportError or OSError`, which evaluates to
        # `except ImportError` -- it does not catch the OSError a present-but-
        # unloadable cv2 raises (a missing libGL, say). Worth knowing, not worth
        # relying on: the headless build in the image has no such libGL need.
        "impl": [
            "emissor",
            "soundfile",
            "cltl-backend"
        ],
        "service": [
            "flask"
        ],
        "client": [
            "requests"
        ]
    }
)
