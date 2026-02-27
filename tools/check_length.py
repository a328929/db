import ast
import os

def analyze_complexity(directory):
    print(f"{'File':<30} | {'Function':<35} | {'Lines':<6}")
    print("-" * 75)
    
    for root, _, files in os.walk(directory):
        for file in files:
            if not file.endswith('.py'):
                continue
            path = os.path.join(root, file)
            with open(path, 'r', encoding='utf-8') as f:
                try:
                    content = f.read()
                    tree = ast.parse(content)
                except SyntaxError:
                    continue

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if hasattr(node, 'end_lineno') and hasattr(node, 'lineno'):
                        lines = node.end_lineno - node.lineno + 1
                        if lines > 60:  # Threshold for 'too long'
                            rel_path = os.path.relpath(path, directory)
                            print(f"{rel_path:<30} | {node.name:<35} | {lines:<6}")

analyze_complexity('/root/db/tg_harvest')
