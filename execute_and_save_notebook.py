"""
Executes the F1 statistical notebook and saves the output (with all cell outputs embedded).
Run from the project root: python execute_and_save_notebook.py
"""
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor, CellExecutionError

NOTEBOOK_PATH = "notebooks/f1_statistical_analysis.ipynb"
PROJECT_ROOT  = r"c:\Users\mdiam\Documents\f1_data_engine"

with open(NOTEBOOK_PATH, encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

ep = ExecutePreprocessor(timeout=300, kernel_name='python3')

try:
    ep.preprocess(nb, {'metadata': {'path': PROJECT_ROOT}})
    with open(NOTEBOOK_PATH, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)
    print(f"Notebook executed and saved to: {NOTEBOOK_PATH}")
except CellExecutionError as e:
    print("Cell execution error:")
    print(str(e)[:5000])
except Exception as e:
    import traceback
    traceback.print_exc()
