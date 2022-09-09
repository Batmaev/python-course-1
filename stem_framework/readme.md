## Development install

1. Install Python

2. You may want to create [a virtual environment](https://docs.python.org/3/tutorial/venv.html)

3. Install the packages we use in development which are listed in `development-requirements.txt`:
    ```sh
    cd ./stem_framework

    # (activate virtual environment)

    pip install -r development-requirements.txt
    ```

4. Install the stem package in editable mode:
    ```sh
    pip install -e .
    ```
    Since we use `pyproject.toml`, you will need pip >= 21.3 (2021-10-11) for this

5. Done. You should be able to run
    ```python
    import stem
    stem.SayHi()
    ```
6. You may build the docs by sphinx with
    ```sh
    cd ./docs
    make html
    ```
    After that, documentation in html format should appear at `stem_framework/docs/build/html/index.html`

7. Alternatively, you can build the docs with `setup.py`:
    ```sh
    python setup.py build_sphinx
    ```
    The second method isn't needed and is added only because the teacher requires it.