
# **PRD: Semantic Engine Browser Playground (React + Vite SPA)**

### *(Updated to use the existing parser-combinator grammar for all DSL features)*

---

# **1. Overview**

### **1.1 Product Summary**

The Semantic Engine Playground is a browser-deliverable **React + Vite** single-page application (SPA) deployed to **GitHub Pages**. It serves as an interactive environment for exploring and debugging the **semantic metrics engine**. Users can:

* Upload JSON data sets
* Define and edit the **semantic schema** (facts, dimensions, attributes, joins)
* Define **metrics using the engine’s DSL**, parsed by the existing parser-combinator grammar
* Write **multiple queries**, also using the same DSL
* Execute queries client-side and view results
* Import/export the entire workspace (data + schema + metrics + queries)

The playground is a self-contained demo and debugging tool—no backend required.

---

# **2. Functional Requirements**

---

## **2.1 Data Layer**

### **2.1.1 JSON Data Input**

Users can import data in two ways:

* **Upload a `.json` file**
* **Paste JSON data** into an editor pane

Requirements:

* Support multiple tables (user provides table name)
* Validate that each table is an array of objects
* Detect basic data types per column for display

### **2.1.2 Data Preview**

* Tabular preview for each table
* First 100 rows displayed, scrollable
* Show inferred column types

---

## **2.2 Schema Editor**

Users can define the semantic schema consumed by the semantic engine.

### **2.2.1 Schema Elements**

* **Facts** (mapped to data tables)
* **Dimensions** (mapped to data tables)
* **Attributes**

  * logical name → physical column
  * default base fact
* **Join Graph**

  * FK → PK mappings
  * reachability validation

### **2.2.2 UI Requirements**

* Sidebar listing schema elements
* Form-based editors for facts, dimensions, attributes, joins
* Read-only JSON preview showing the generated schema structure

### **2.2.3 Validation**

* Attribute → column resolution
* Join correctness (referencing existing columns)
* Reachability per fact
* Highlight cycles or ambiguous paths

---

## **2.3 Metric Editor**

### **2.3.1 Canonical DSL Requirement**

**Metric definitions are authored using the *existing semantic engine DSL*, parsed through the project’s parser-combinator grammar.**
The playground does **not** define or invent new DSL syntax.

The grammar module is the **single source of truth** for:

* Syntax rules
* Expression structure
* Aggregate syntax
* Metric references
* Transform calls
* Validation logic

### **2.3.2 Editor Features**

The metric editor must provide:

* Monaco-based editor
* Syntax highlighting (based on our DSL grammar tokens)
* Autocomplete for:

  * metrics
  * attributes
  * transforms
  * aggregate functions
* Inline error markers:

  * Syntax errors from the parser
  * Semantic errors (e.g., unresolved metric references)

### **2.3.3 Metric Types Supported**

The editor must support all metric types defined by the grammar:

* **Aggregate metrics**
* **Expression metrics** (row-level expressions)
* **Derived metrics** (composed of other metrics)
* **Transforms** (expression-based or table-based, as supported by the existing runtime)

### **2.3.4 Parsing & Validation**

* The playground must call the **existing parser combinator** to obtain an AST
* Errors returned from the parser must be surfaced as Monaco markers
* Runtime validation (AST → semantic definition) must highlight issues such as:

  * Unknown attributes
  * Unknown metrics
  * Circular dependencies
  * Grain mismatches

### **2.3.5 Metadata Panel**

For each parsed metric:

* Show parsed AST (JSON, read-only)
* Show dependency list
* Show base fact (if resolved)
* Show effective grain
* Show compiled form (as used by the engine runtime)

---

## **2.4 Query Editor**

### **2.4.1 Query DSL**

Queries are also authored in the **existing DSL**, parsed by the same parser module.
(Select lists, filters, expressions, ordering, limits, etc.)

### **2.4.2 Features**

* Multi-query support
* Monaco editor with DSL syntax highlighting
* Parser-combinator integration for immediate error feedback
* Autocomplete for:

  * attributes
  * metrics
  * facts
  * dimensions

### **2.4.3 Execution**

* Query execution is fully client-side
* Engine uses the schema + metrics + data in memory
* Results displayed in tabular + JSON views

### **2.4.4 Error Handling**

* Syntax errors from parser
* Semantic resolution issues (unknown identifiers, join failures, metric issues)
* Displayed in a “Problems” panel

---

## **2.5 Import and Export**

### **2.5.1 Export Structure**

A workspace export contains:

```json
{
  "version": "1.x",
  "data": { "tableName": [...] },
  "schema": { ... },
  "metrics": {
    "metricName": "raw DSL text"
  },
  "queries": {
    "queryName": "raw DSL text"
  }
}
```

### **2.5.2 Import**

* Restore workspace fully
* Apply parser to each metric/query
* Surface any errors via UI

### **2.5.3 Autosave**

* LocalStorage autosave for all user edits

---

# **3. Non-Functional Requirements**

## **3.1 Architecture**

* React + Vite + TypeScript
* Monaco Editor
* LocalStorage for persistence
* Shared parser-combinator grammar module imported directly
* No backend; pure SPA
* Deployment via GitHub Pages

## **3.2 Performance**

* Handle datasets up to ~10MB
* Query execution <500ms typical

## **3.3 Security**

* **No JS execution from DSL**; DSL → AST → controlled evaluation
* Sandboxed module boundaries

---

# **4. UI/UX Requirements**

## **4.1 Layout**

IDE-style three-pane layout:

| Left Sidebar                | Center Workspace | Right Sidebar                              |
| --------------------------- | ---------------- | ------------------------------------------ |
| Schema/Data/Metrics/Queries | Editor (Monaco)  | Parsed AST, Metadata, Data Preview, Errors |

## **4.2 Results Panel**

* Switchable table / JSON view
* Execution log
* Warnings related to semantics

---

# **5. Technical Components**

### **5.1 Components**

* **Workspace Manager** (state for data, schema, metrics, queries)
* **Metric Parser Adapter** (calls parser-combinator and transforms AST)
* **Query Parser Adapter**
* **Engine Runner** (executes queries using semantic engine runtime)
* **Schema Editor Forms**
* **Table Previewer**

### **5.2 Parser Integration**

* The parser is imported and invoked directly
* Receiver of parser errors must map tokens → Monaco editor ranges

---

# **6. Milestones**

### **MVP**

* Data import
* Schema editing
* Metric editing (DSL + parser integration)
* Single-query execution

### **Full Playground**

* Multi-query support
* Export/import
* Transform support
* Error panel

### **Advanced**

* Graph view of schema joins
* PWA offline mode
* DSL auto-completion powered by parser productions

---

# **7. Out of Scope**

* Authentication
* Remote storage
* Large-scale data (>10MB)
* Concurrent editing/collaboration

---

# **8. Open Questions**

1. Should metrics and queries share a unified AST serialization for export?
2. Should we expose the parser tokens/types in the UI for debugging?
3. Should the playground offer templates/snippets for the DSL (still using correct grammar)?

