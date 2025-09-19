#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "pandas",
#   "networkx",
#   "matplotlib",
# ]
# ///
"""
CSV Merge Analyzer - Graph-based CSV merging tool

Just run: uv run csv_merge_analyzer.py
(uv will automatically handle dependencies and virtual environment)
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Set

class CSVMergeAnalyzer:
    def __init__(self, csv_directory: str):
        self.csv_directory = Path(csv_directory)
        self.csvs = {}
        self.column_graph = nx.Graph()
        self.csv_graph = nx.Graph()
        
    def load_csvs(self) -> None:
        """Load all CSV files and their column information."""
        csv_files = list(self.csv_directory.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                # Just read the header to get column names
                df = pd.read_csv(csv_file, nrows=0)
                self.csvs[csv_file.stem] = {
                    'path': csv_file,
                    'columns': set(df.columns),
                    'column_list': list(df.columns)
                }
                print(f"Loaded {csv_file.stem}: {len(df.columns)} columns")
            except Exception as e:
                print(f"Error loading {csv_file}: {e}")
    
    def build_connection_graph(self) -> None:
        """Build a graph where nodes are CSVs and edges are shared columns."""
        csv_names = list(self.csvs.keys())
        
        for i, csv1 in enumerate(csv_names):
            for csv2 in csv_names[i+1:]:
                shared_cols = self.csvs[csv1]['columns'] & self.csvs[csv2]['columns']
                if shared_cols:
                    # Add edge with shared columns as attribute
                    self.csv_graph.add_edge(csv1, csv2, shared_columns=list(shared_cols))
        
        # Add nodes that might not have connections
        for csv_name in csv_names:
            if csv_name not in self.csv_graph:
                self.csv_graph.add_node(csv_name)
    
    def analyze_coverage(self) -> Dict:
        """Analyze the connection coverage between CSVs."""
        analysis = {
            'csv_count': len(self.csvs),
            'connections': [],
            'isolated_csvs': [],
            'all_columns': set(),
            'shared_columns': defaultdict(list),
            'merge_path': None,
            'is_mergeable': False
        }
        
        # Collect all columns
        for csv_info in self.csvs.values():
            analysis['all_columns'].update(csv_info['columns'])
        
        # Analyze connections
        for edge in self.csv_graph.edges(data=True):
            csv1, csv2, data = edge
            shared_cols = data['shared_columns']
            analysis['connections'].append({
                'csv1': csv1,
                'csv2': csv2,
                'shared_columns': shared_cols
            })
            
            # Track which CSVs share each column
            for col in shared_cols:
                analysis['shared_columns'][col].extend([csv1, csv2])
        
        # Find isolated CSVs (no shared columns with others)
        analysis['isolated_csvs'] = [
            csv for csv in self.csvs.keys() 
            if csv not in self.csv_graph or self.csv_graph.degree(csv) == 0
        ]
        
        # Check if all CSVs can be connected (is there a spanning tree?)
        if len(self.csv_graph.nodes) > 0:
            analysis['is_mergeable'] = nx.is_connected(self.csv_graph)
            if analysis['is_mergeable']:
                # Find a merge path (spanning tree)
                analysis['merge_path'] = self.find_merge_path()
        
        return analysis
    
    def find_merge_path(self) -> List[Tuple]:
        """Find an optimal path to merge all CSVs."""
        if not nx.is_connected(self.csv_graph):
            return None
        
        # Use minimum spanning tree to find optimal merge order
        mst = nx.minimum_spanning_tree(self.csv_graph)
        
        # Convert MST to merge operations
        merge_path = []
        edges = list(mst.edges(data=True))
        
        for csv1, csv2, data in edges:
            shared_cols = data['shared_columns']
            merge_path.append({
                'merge': [csv1, csv2],
                'on_columns': shared_cols
            })
        
        return merge_path
    
    def visualize_connections(self) -> None:
        """Create a visualization of CSV connections."""
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(self.csv_graph, k=3, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(self.csv_graph, pos, node_color='lightblue', 
                              node_size=3000, alpha=0.7)
        
        # Draw edges with labels
        nx.draw_networkx_edges(self.csv_graph, pos, alpha=0.5)
        
        # Draw labels
        nx.draw_networkx_labels(self.csv_graph, pos, font_size=10)
        
        # Draw edge labels (shared columns)
        edge_labels = {}
        for edge in self.csv_graph.edges(data=True):
            csv1, csv2, data = edge
            shared_cols = data['shared_columns']
            edge_labels[(csv1, csv2)] = ', '.join(shared_cols[:2])  # Show first 2 columns
            if len(shared_cols) > 2:
                edge_labels[(csv1, csv2)] += f" (+{len(shared_cols)-2})"
        
        nx.draw_networkx_edge_labels(self.csv_graph, pos, edge_labels, font_size=8)
        
        plt.title("CSV Connection Graph\n(Nodes=CSVs, Edges=Shared Columns)")
        plt.axis('off')
        plt.tight_layout()
        plt.show()
    
    def execute_merge(self, output_file: str = "merged_data.csv") -> pd.DataFrame:
        """Execute the merge based on the analysis."""
        analysis = self.analyze_coverage()
        
        if not analysis['is_mergeable']:
            print("❌ Cannot merge all CSVs - they're not fully connected!")
            print(f"Isolated CSVs: {analysis['isolated_csvs']}")
            return None
        
        merge_path = analysis['merge_path']
        if not merge_path:
            print("❌ No merge path found!")
            return None
        
        print("🔄 Executing merge...")
        
        # Load the first two CSVs to merge
        first_merge = merge_path[0]
        csv1_name, csv2_name = first_merge['merge']
        on_cols = first_merge['on_columns']
        
        df1 = pd.read_csv(self.csvs[csv1_name]['path'])
        df2 = pd.read_csv(self.csvs[csv2_name]['path'])
        
        # Smart merge function to avoid column conflicts
        def smart_merge(left_df, right_df, on_columns):
            # Get unique columns from right df
            right_unique_cols = [col for col in right_df.columns 
                               if col not in left_df.columns or col in on_columns]
            return pd.merge(left_df, right_df[right_unique_cols], 
                           on=on_columns, how='outer')
        
        # Start with first merge
        result = smart_merge(df1, df2, on_cols)
        merged_csvs = {csv1_name, csv2_name}
        
        print(f"✅ Merged {csv1_name} + {csv2_name} on {on_cols}")
        
        # Continue with remaining merges
        for merge_op in merge_path[1:]:
            csv1_name, csv2_name = merge_op['merge']
            on_cols = merge_op['on_columns']
            
            # Determine which CSV to load (the one not already merged)
            if csv1_name in merged_csvs:
                next_csv = csv2_name
            else:
                next_csv = csv1_name
            
            next_df = pd.read_csv(self.csvs[next_csv]['path'])
            result = smart_merge(result, next_df, on_cols)
            merged_csvs.add(next_csv)
            
            print(f"✅ Added {next_csv} on {on_cols}")
        
        # Save result
        result.to_csv(output_file, index=False)
        print(f"💾 Saved merged data to {output_file}")
        print(f"📊 Final shape: {result.shape}")
        
        return result

# Example usage
if __name__ == "__main__":
    print("🔍 CSV Merge Analyzer")
    print("=" * 50)
    
    # Prompt for directory path
    while True:
        csv_dir = input("\n📁 Enter path to directory containing CSV files: ").strip()
        
        if not csv_dir:
            print("❌ Please enter a valid path")
            continue
            
        # Handle common path issues
        if csv_dir.startswith('"') and csv_dir.endswith('"'):
            csv_dir = csv_dir[1:-1]  # Remove quotes
        
        csv_path = Path(csv_dir)
        if not csv_path.exists():
            print(f"❌ Directory '{csv_dir}' not found")
            continue
        
        if not csv_path.is_dir():
            print(f"❌ '{csv_dir}' is not a directory")
            continue
            
        # Check if directory contains CSV files
        csv_files = list(csv_path.glob("*.csv"))
        if not csv_files:
            print(f"❌ No CSV files found in '{csv_dir}'")
            continue
            
        print(f"✅ Found {len(csv_files)} CSV files")
        break
    
    # Initialize analyzer
    analyzer = CSVMergeAnalyzer(csv_dir)
    
    # Load and analyze
    print("\n🔄 Loading CSV files...")
    analyzer.load_csvs()
    
    print("🔄 Building connection graph...")
    analyzer.build_connection_graph()
    
    # Get analysis
    analysis = analyzer.analyze_coverage()
    
    print("\n" + "=" * 50)
    print("📊 ANALYSIS RESULTS")
    print("=" * 50)
    print(f"📁 CSV files found: {analysis['csv_count']}")
    print(f"🔗 Connections found: {len(analysis['connections'])}")
    print(f"📋 Total unique columns: {len(analysis['all_columns'])}")
    
    if analysis['isolated_csvs']:
        print(f"🏝️  Isolated CSVs (no shared columns): {', '.join(analysis['isolated_csvs'])}")
    
    print(f"✅ Can merge all CSVs: {analysis['is_mergeable']}")
    
    if analysis['connections']:
        print(f"\n🔗 CONNECTIONS FOUND:")
        for conn in analysis['connections']:
            shared = ', '.join(conn['shared_columns'])
            print(f"   • {conn['csv1']} ↔ {conn['csv2']} via: {shared}")
    
    if analysis['is_mergeable']:
        print(f"\n📋 OPTIMAL MERGE PATH:")
        for i, step in enumerate(analysis['merge_path']):
            on_cols = ', '.join(step['on_columns'])
            print(f"   {i+1}. Merge {step['merge'][0]} + {step['merge'][1]} on: {on_cols}")
        
        # Ask if user wants to execute merge
        print("\n" + "=" * 50)
        execute = input("🚀 Execute merge? (y/n): ").strip().lower()
        
        if execute in ['y', 'yes']:
            output_name = input("💾 Output filename (press Enter for 'merged_data.csv'): ").strip()
            if not output_name:
                output_name = "merged_data.csv"
            elif not output_name.endswith('.csv'):
                output_name += '.csv'
            
            print(f"\n🔄 Merging CSVs into '{output_name}'...")
            merged_df = analyzer.execute_merge(output_name)
            
            if merged_df is not None:
                print(f"\n🎉 SUCCESS! Merged data saved to '{output_name}'")
                print(f"📊 Final dataset: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
        else:
            print("👍 Analysis complete. Merge not executed.")
    else:
        print("\n❌ Cannot merge all CSVs - some are not connected!")
        print("💡 Suggestion: Check if isolated CSVs share columns with others.")
    
    print("\n✨ Done!")