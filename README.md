# Claims Processing Pipeline

Hi! This is a simple PySpark project that processes insurance claims. It takes raw data, cleans it up, and saves the results.

## What's Inside?
- **PySpark**: We use this to handle big data easily.
- **Configurable**: You can change settings in `config/spark.conf`.
- **Logging**: We keep track of everything in the `logs/` folder, so you know what's happening.

## How to Run It
1. **Install what you need**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the script**:
   ```bash
   python main.py
   ```

3. **See the results**:
   - Check the `processed_claims_output` folder for the data.
   - Check `logs/` to see the run details.

That's it!
