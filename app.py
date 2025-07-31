

from flask import Flask, request, render_template, redirect, url_for, send_from_directory, jsonify
import os
import pandas as pd
from werkzeug.utils import secure_filename
from comparator import CSVComparator  # Importing the CSVComparator class

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['OUTPUT_FOLDER'] = 'output_files/'
app.config['ALLOWED_EXTENSIONS'] = {'csv'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    description = (
        "This tool allows you to upload two CSV files for comparison. "
        "Supported files are in .csv format, with options to specify the start row for data and columns to compare. "
        "You can also choose the comparison order to control which file is prioritized in the comparison."
    )

    if request.method == 'POST':
        # Get files and form data
        csv1_file = request.files.get('csv1_file')
        csv2_file = request.files.get('csv2_file')
        columns = request.form.get('columns')
        data_start_row_csv1 = int(request.form.get('data_start_row_csv1') or 0)
        data_start_row_csv2 = int(request.form.get('data_start_row_csv2') or 0)
        comparison_order = int(request.form.get('order') or 1)

        if not csv1_file or not csv2_file or not allowed_file(csv1_file.filename) or not allowed_file(csv2_file.filename):
            return render_template('upload.html', error_message="Please upload valid CSV files.")

        # Save CSV files
        csv1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv1_file.filename))
        csv2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv2_file.filename))
        csv1_file.save(csv1_path)
        csv2_file.save(csv2_path)

        # Run CSV comparison
        comparator = CSVComparator(csv1_path, csv2_path, columns, data_start_row_csv1, data_start_row_csv2, comparison_order)
        comparator.run_comparison()

        generated_files = ['matching_rows.csv', 'non_matching_rows.csv', 'comparison_stats.csv']
        return render_template('results.html', files=generated_files)

    return render_template('upload.html', description=description)

def detect_delimiter(file_path):
    """Detect the delimiter used in the CSV file"""
    common_delimiters = [',', '^', ';', '\t', '|']
    with open(file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline()
        for delimiter in common_delimiters:
            if delimiter in first_line:
                return delimiter
    return ','  # Default to comma if no delimiter is detected

@app.route('/get_columns', methods=['POST'])
def get_columns():
    """Extract column names from uploaded CSV files"""
    try:
        csv_file = request.files.get('csv_file')
        data_start_row = int(request.form.get('data_start_row', 0))
        
        if not csv_file or not allowed_file(csv_file.filename):
            return jsonify({'error': 'Invalid CSV file'}), 400
        
        # Save temporary file
        temp_filename = 'temp_' + secure_filename(csv_file.filename)
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
        csv_file.save(temp_path)
        
        # Detect delimiter and read CSV
        delimiter = detect_delimiter(temp_path)
        df = pd.read_csv(temp_path, header=data_start_row, delimiter=delimiter, low_memory=False)
        
        # Get column names and normalize them
        columns = df.columns.str.lower().str.replace(" ", "").str.strip().tolist()
        
        # Clean up temp file
        os.remove(temp_path)
        
        return jsonify({
            'columns': columns,
            'delimiter': delimiter,
            'total_rows': len(df)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/downloads/<path:filename>', methods=['GET'])
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
