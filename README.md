# CSV Merge Tools 📊

A collection of Python tools for intelligently merging CSV files using graph-based analysis and duplicate detection.

## 🚀 Quick Start

No setup required! Just run the tools directly with uv:

```bash
# Main merge tool
uv run csv_merge_analyzer.py

# Debug duplicate issues
uv run merge_debugger.py
```

## 🛠️ Tools Included

### 1. CSV Merge Analyzer (`csv_merge_analyzer.py`)

A graph-based tool that automatically discovers how to merge multiple CSV files by finding shared columns.

**Features:**
- 🔍 **Auto-discovery**: Scans directory and finds all shared columns between CSVs
- 📊 **Graph analysis**: Treats merging as a graph problem (CSVs = nodes, shared columns = edges)
- 🛤️ **Optimal path**: Uses minimum spanning tree to find the best merge order
- 🔧 **Smart merging**: Prevents column conflicts by only keeping unique columns
- ✅ **Validation**: Identifies isolated CSVs that can't be connected
- 🎯 **Interactive**: Prompts for directory and output preferences

**How it works:**
1. Scans a directory for CSV files
2. Builds a connection graph based on shared columns
3. Determines if all CSVs can be merged (connected graph)
4. Finds optimal merge path using graph algorithms
5. Executes merge while avoiding column conflicts

### 2. Merge Debugger (`merge_debugger.py`)

A diagnostic tool for troubleshooting merge issues, especially duplicate records.

**Features:**
- 🔎 **Duplicate detection**: Finds duplicate values in potential join columns
- 🧹 **Data cleaning hints**: Identifies whitespace, case, and formatting issues
- 📋 **Step-by-step analysis**: Simulates merges to pinpoint where duplicates occur
- 🎯 **Value inspection**: Shows sample values with types and formatting
- 💡 **Fix suggestions**: Provides concrete solutions for common issues

## 📋 Usage Examples

### Basic Merge Workflow

```bash
# 1. First, run the main analyzer
uv run csv_merge_analyzer.py
```

**Sample output:**
```
📁 Enter path to directory containing CSV files: /path/to/csvs
✅ Found 3 CSV files

📊 ANALYSIS RESULTS
📁 CSV files found: 3
🔗 Connections found: 2
✅ Can merge all CSVs: True

🔗 CONNECTIONS FOUND:
   • sales_data ↔ customer_info via: customer_id
   • customer_info ↔ locations via: location_id

📋 OPTIMAL MERGE PATH:
   1. Merge sales_data + customer_info on: customer_id
   2. Merge merged_result + locations on: location_id

🚀 Execute merge? (y/n): y
```

### Debugging Duplicates

If you get unexpected duplicate records:

```bash
# Run the debugger
uv run merge_debugger.py
```

**Sample output:**
```
🔎 CHECKING FOR DUPLICATES IN JOIN COLUMNS
📋 Checking column 'customer_id':
  ❌ sales_data: 2 duplicates found!
     Duplicate values: ['CUST_123']
     Duplicate rows:
       Row 5: {'customer_id': 'CUST_123', 'amount': 100}
       Row 12: {'customer_id': 'CUST_123', 'amount': 200}
```

## 🔧 Technical Details

### Graph Algorithm

The merge analyzer treats CSV merging as a graph connectivity problem:

- **Nodes**: Individual CSV files
- **Edges**: Shared columns between CSVs
- **Goal**: Find a spanning tree that connects all nodes
- **Solution**: Minimum spanning tree for optimal merge order

### Merge Strategy

The tool uses a "smart merge" approach:

```python
def smart_merge(left_df, right_df, on_columns):
    # Only take unique columns from right dataframe
    right_unique_cols = [col for col in right_df.columns 
                        if col not in left_df.columns or col in on_columns]
    return pd.merge(left_df, right_df[right_unique_cols], 
                   on=on_columns, how='outer')
```

This prevents the common issue of pandas adding `_x` and `_y` suffixes to duplicate columns.

## 🐛 Common Issues & Solutions

### Issue: Getting duplicate records after merge

**Cause**: Duplicate values in join columns create Cartesian products

**Solution**: 
```bash
# Run the debugger to identify duplicates
uv run merge_debugger.py

# Common fixes:
# 1. Remove duplicates before merging
df = df.drop_duplicates(subset=['join_column'])

# 2. Clean whitespace
df['join_column'] = df['join_column'].str.strip()

# 3. Standardize case
df['join_column'] = df['join_column'].str.lower()
```

### Issue: "Cannot merge all CSVs - they're not fully connected"

**Cause**: Some CSVs don't share any columns with others

**Solutions**:
- Check if isolated CSVs have typos in column names
- Verify that you need all CSVs in the merge
- Consider manual preprocessing to create linking columns

### Issue: Wrong merge order causing data loss

**Cause**: The tool finds a valid path but not the optimal one for your use case

**Solution**: The tool uses minimum spanning tree which should be optimal, but you can modify the `find_merge_path()` function for custom logic.

## 📊 Example Directory Structure

```
my_data/
├── sales_q1.csv          # columns: date, customer_id, amount
├── customer_info.csv     # columns: customer_id, name, location_id  
├── locations.csv         # columns: location_id, city, state
└── products.csv          # columns: product_id, name, category
```

**Result**: The tool will merge the first three CSVs (connected via `customer_id` and `location_id`) but identify `products.csv` as isolated.

## 🔄 Dependencies

Both tools use inline dependency management - no manual installation required!

**Auto-installed packages:**
- `pandas` - Data manipulation and merging
- `networkx` - Graph algorithms 
- `matplotlib` - Connection visualization (merge analyzer only)

## 🎯 When to Use Each Tool

**Use CSV Merge Analyzer when:**
- You have multiple CSVs that should be related
- You're not sure how they connect
- You want to merge them automatically
- You want to avoid manual VLOOKUP/JOIN operations

**Use Merge Debugger when:**
- Your merge created unexpected duplicate rows
- Records appear to be missing after merge
- You suspect data quality issues
- You want to validate your data before merging

## 🏗️ Extending the Tools

Both tools are designed to be modifiable:

**Custom merge logic**: Modify the `smart_merge()` function
**Different join types**: Change `how='outer'` to `'inner'`, `'left'`, etc.
**Custom validation**: Add checks in the analysis phase
**Export formats**: Modify output to Excel, JSON, etc.
