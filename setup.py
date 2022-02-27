import setuptools

PACKAGE = "flippr"
VERSION = "1.0"

setuptools.setup(
    name=PACKAGE,
    packages=["flippr/utils"],
    verion=VERSION,
    author="whjackso23",
    author_email="whjackso23@gmail.com",
    description="ticket price forecasting package",
    long_description_content_type="text/markdown",
    url="https://github.com/whjackso23/flippr"
)