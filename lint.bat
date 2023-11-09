flake8 eedl --ignore="W191,E501,E128,E124,E126,E127"
flake8 tests --ignore="W191,E501,E128,E124,E126,E127"
flake8 examples --ignore="W191,E501,E128,E124,E126,E127"
flake8 docs/source --ignore="W191,E501,E128,E124,E126,E127"
mypy eedl --enable-incomplete-feature=Unpack
