---
description: Bug Handling Workflow Rules
alwaysApply: true
---
# Bug Handling Workflow Rules

**PURPOSE:** Systematic methodology for identifying, analyzing, and resolving bugs efficiently using evidence-based approaches and collaborative tools.

**WHEN TO APPLY:** When encountering errors, warnings, unexpected behavior, test failures, or any code malfunction.

**CORE PRINCIPLE:** Root Cause Analysis (RCA) - Solve problems at their source, not just symptoms. Never guess; research, analyze, verify.

---

## 🎯 QUICK DECISION TREE

```
Bug/Error Detected
    ↓
Can you reproduce it? → NO → Gather more info (logs, user reports, conditions)
    ↓ YES
Is error message clear? → YES → Go to Phase 2 (Locate)
    ↓ NO
Is it intermittent/flaky? → YES → Add logging, monitor patterns
    ↓ NO
Follow full workflow below ↓
```

---

## 📋 THE 5-PHASE WORKFLOW

### PHASE 1: REPRODUCE & UNDERSTAND

**Goal:** Confirm the bug exists and understand its scope

**Steps:**
1. **Reproduce Consistently**
   - Document exact steps to trigger the bug
   - Note conditions: OS, browser, inputs, environment variables, timing
   - Try to reproduce in different environments (dev/staging/prod)
   - Success criteria: Can reproduce bug ≥3 times with same steps

2. **Gather Evidence**
   ```
   COLLECT:
   - Error messages (full stack traces)
   - Log files (application + system logs)
   - Screenshots/recordings of the issue
   - User reports (what they expected vs. what happened)
   - Recent changes (git log, recent commits, PRs merged)
   - Environment details (versions, dependencies, configs)
   ```

3. **Define Bug Characteristics**
   - **Severity:** Critical / High / Medium / Low
   - **Scope:** Single function / Module / System-wide
   - **Type:** Syntax / Logic / Runtime / Integration / Performance
   - **Frequency:** Always / Intermittent / Edge case

**Tools:**
- Cursor Diagnostics Panel (for linter/compiler errors)
- Terminal (for running tests, viewing logs)
- Git history (`git log`, `git blame`)

**Output:** Clear bug report with reproduction steps

---

### PHASE 2: LOCATE ROOT CAUSE

**Goal:** Pinpoint the exact code location causing the bug

**Strategy: Use Serena MCP + Cursor for Efficient Investigation**

#### A. Start with Error Message/Stack Trace

If you have an error message:

```python
# Example: "AttributeError: 'NoneType' object has no attribute 'process'"

# STEP 1: Find where the error occurs (file + line number from stack trace)
# STEP 2: Use Serena to understand context
mcp_serena_get_symbols_overview("path/to/error_file.py")

# STEP 3: Find the problematic symbol
mcp_serena_find_symbol(
    name_path="ClassName/method_name",
    relative_path="path/to/error_file.py",
    include_body=True,
    depth=0
)

# STEP 4: Check who calls this method (trace data flow)
mcp_serena_find_referencing_symbols(
    name_path="ClassName/method_name",
    relative_path="path/to/error_file.py"
)
```

#### B. If Error Location is Unknown

Use systematic search:

```python
# OPTION 1: Search for error-related patterns
mcp_serena_search_for_pattern(
    substring_pattern=r"def process.*:",  # Find all process methods
    relative_path="src/",
    paths_include_glob="*.py",
    context_lines_before=3,
    context_lines_after=3
)

# OPTION 2: Search for specific keywords from error
# Example: Error mentions "database connection"
mcp_serena_search_for_pattern(
    substring_pattern=r"database.*connection",
    relative_path="",
    context_lines_before=5,
    context_lines_after=5
)

# OPTION 3: Use Cursor's built-in search for exact strings
# Cursor Search Panel: Search for error message strings
```

#### C. Understand Dependencies and Data Flow

```python
# Find what calls the buggy function (upstream)
mcp_serena_find_referencing_symbols(name, path)

# Find what the buggy function calls (downstream)
mcp_serena_find_symbol(name, path, include_body=True, depth=1)

# Check related configuration
read_file("config/settings.py", start_line=X, end_line=Y)
```

#### D. Isolate the Problem

**Binary Search Approach:**
1. Comment out half of the suspicious code
2. Run tests/reproduce bug
3. If bug persists → problem is in active half
4. If bug disappears → problem is in commented half
5. Repeat until you isolate exact lines

**Tools:**
- Serena MCP: `find_symbol`, `find_referencing_symbols`, `get_symbols_overview`, `search_for_pattern`
- Cursor: Breakpoints, Debug Console, Step Through Execution
- Terminal: Print statements, logging, test runners

**Output:** Exact file, function, and lines causing the bug

---

### PHASE 3: RESEARCH SOLUTIONS

**Goal:** Find how others solved similar issues or discover novel solutions

#### A. Search for Similar Issues (Use MCP Search Tools)

**Strategy: Parallel Research from Multiple Sources**

```python
# PARALLEL EXECUTION (run all in same tool call batch)

# 1. General web search for the error
mcp_brave-search_brave_web_search(
    query="[exact error message] [programming language] [framework]",
    count=10
)

# 2. Search for code examples
mcp_exa_get_code_context_exa(
    query="how to fix [error type] in [framework/library]",
    tokensNum=3000
)

# 3. Search GitHub for similar issues
mcp_octocode_githubSearchCode(
    queries=[{
        "keywordsToSearch": ["error_keyword", "fix"],
        "owner": "relevant_org",
        "repo": "relevant_repo",
        "match": "file",
        "limit": 5
    }]
)

# 4. Search official documentation
mcp_context7_resolve-library-id(libraryName="your-library")
# Then:
mcp_context7_get-library-docs(
    context7CompatibleLibraryID="/org/project",
    topic="error handling troubleshooting"
)
```

**Research Checklist:**
- [ ] Check Stack Overflow for exact error message
- [ ] Search GitHub Issues in relevant repos
- [ ] Read official documentation (error handling sections)
- [ ] Look for blog posts/tutorials on the topic
- [ ] Check community forums (Reddit, Discord, Slack)

#### B. Deep Content Extraction (for complex bugs)

```python
# If you find promising URLs, extract full content
mcp_Bright_Data_scrape_batch(
    urls=["url1", "url2", "url3"]  # Max 10 URLs
)
```

#### C. Analyze Solutions Found

**Evaluation Criteria:**
- **Relevance:** Does it address the exact same error?
- **Recency:** Is the solution up-to-date with current versions?
- **Authority:** Is the source trustworthy (official docs, experienced devs)?
- **Completeness:** Does it explain WHY, not just WHAT?
- **Verification:** Did others confirm it works?

**Document Findings:**
```
SOLUTION CANDIDATES:

1. Solution from [Source]:
   - Approach: [Description]
   - Why it works: [Root cause explanation]
   - Pros: [Advantages]
   - Cons: [Limitations]
   - Confidence: High/Medium/Low

2. Solution from [Source]:
   [Same structure]
```

#### D. If No Existing Solution Found

**Deep Dive Strategy:**
1. **Read Official Documentation** thoroughly
   - API references
   - Error handling guides
   - Migration guides (if version-related)
   - Known issues / FAQs

2. **Analyze Library Source Code**
   ```python
   # Find the library's implementation
   mcp_octocode_githubSearchCode(
       queries=[{
           "keywordsToSearch": ["error_message_keyword"],
           "owner": "library_org",
           "repo": "library_repo",
           "match": "file"
       }]
   )
   
   # Read the actual implementation
   mcp_octocode_githubGetFileContent(
       queries=[{
           "owner": "library_org",
           "repo": "library_repo",
           "path": "src/error_module.py",
           "matchString": "def error_function"
       }]
   )
   ```

3. **Understand Expected Behavior**
   - What SHOULD happen vs. what IS happening?
   - What assumptions does the code make?
   - What preconditions must be met?

4. **Hypothesis Formation** (use `sequential-thinking mcp`)
   - List possible root causes
   - Rank by likelihood
   - Test each hypothesis systematically

**Output:** List of solution candidates with analysis

---

### PHASE 4: IMPLEMENT FIX

**Goal:** Apply the solution correctly without introducing new bugs

#### A. Pre-Implementation Checklist

```python
# BEFORE editing code, always:
mcp_serena_think_about_task_adherence()

# Check impact of your changes:
mcp_serena_find_referencing_symbols(
    name_path="function_to_modify",
    relative_path="path/to/file.py"
)
# This shows ALL places that call this function!
```

- [ ] Understand the root cause (not just symptoms)
- [ ] Have a clear solution approach
- [ ] Know which files/functions to modify
- [ ] Identified all affected code paths
- [ ] Backed up current state (`git stash` or commit)

#### B. Implementation Best Practices

1. **Make Minimal Changes**
   - Change only what's necessary
   - Avoid "while I'm here" refactors (do separately)
   - One bug = one fix = one commit

2. **Add Defensive Code**
   ```python
   # Before (buggy):
   result = data.process()
   
   # After (defensive):
   if data is None:
       logger.error("Data is None in process_data()")
       raise ValueError("Expected data, got None")
   result = data.process()
   ```

3. **Add Logging**
   ```python
   # Help future debugging
   logger.debug(f"Processing data: {data}")
   result = data.process()
   logger.info(f"Process completed: {result}")
   ```

4. **Document the Fix**
   ```python
   # Add comments explaining WHY, not WHAT
   # Fix for issue #123: NoneType error when data is empty
   # Root cause: API can return null when no results found
   # Solution: Add null check before processing
   if data is None:
       return default_value
   ```

5. **Follow Existing Code Style**
   - Match indentation, naming conventions
   - Use project's linting rules
   - Run formatter before committing

#### C. Testing Your Fix

**Verification Levels:**

1. **Unit Test** (if applicable)
   ```bash
   # Run specific test
   pytest tests/test_buggy_module.py::test_specific_case -v
   ```

2. **Reproduce Original Bug**
   - Follow exact reproduction steps
   - Bug should NO LONGER occur
   - Check edge cases too

3. **Regression Testing**
   ```bash
   # Run full test suite
   pytest tests/ -v
   # Or
   npm test
   ```

4. **Check for New Errors**
   ```python
   # Use Cursor Diagnostics Panel
   read_lints(["path/to/modified/file.py"])
   ```

5. **Manual Testing**
   - Test the happy path
   - Test error cases
   - Test boundary conditions

**Output:** Working fix that solves the bug without side effects

---

### PHASE 5: DOCUMENT & PREVENT

**Goal:** Ensure the bug doesn't happen again and share knowledge

#### A. Document the Solution

**Commit Message Format:**
```
fix: [Brief description of bug]

Problem:
- [What was broken]
- [How it manifested]

Root Cause:
- [Why it happened]

Solution:
- [What you changed]
- [Why this fixes it]

Tested:
- [How you verified the fix]

References:
- Fixes #issue_number
- Related to [Stack Overflow link / documentation]
```

#### B. Update Project Memory (if architectural insight gained)

```python
# If the bug revealed important architectural knowledge:
mcp_serena_write_memory(
    memory_name="common-bugs-and-solutions",
    content="""
    ## [Bug Type]: [Brief Title]
    
    **Symptom:** [Error message / behavior]
    **Root Cause:** [Why it happens]
    **Solution:** [How to fix]
    **Prevention:** [How to avoid in future]
    **Related Files:** [Key files involved]
    """
)
```

#### C. Add Preventive Measures

1. **Add Tests**
   ```python
   # Add regression test to prevent future occurrence
   def test_handles_none_data():
       """Regression test for issue #123"""
       result = process_data(None)
       assert result == default_value
   ```

2. **Add Validation**
   ```python
   # Add input validation
   def process_data(data):
       if not isinstance(data, ExpectedType):
           raise TypeError(f"Expected ExpectedType, got {type(data)}")
       # ... rest of function
   ```

3. **Improve Error Messages**
   ```python
   # Before: Generic error
   raise ValueError("Invalid data")
   
   # After: Helpful error
   raise ValueError(
       f"Invalid data format: expected dict with 'id' and 'name', "
       f"got {type(data)} with keys {data.keys() if isinstance(data, dict) else 'N/A'}"
   )
   ```

4. **Update Documentation**
   - Add comments in code
   - Update README if it affects usage
   - Update API docs if public-facing

**Output:** Well-documented fix with preventive measures

---

## 🛠️ TOOL DECISION MATRIX

| Task | Primary Tool | When to Use | Command Example |
|---|---|---|---|
| **Find error location** | Stack trace + Serena | Have error message | `get_symbols_overview` → `find_symbol` |
| **Understand code flow** | Serena | Need to trace execution | `find_referencing_symbols` |
| **Search for patterns** | Serena | Error location unknown | `search_for_pattern` |
| **Research solutions** | Brave + Exa | Common errors | `brave_web_search` + `exa_get_code_context` |
| **Deep content extraction** | Bright Data | Complex research | `scrape_batch` on promising URLs |
| **Check documentation** | Context7 | Library/framework errors | `get-library-docs` |
| **Find code examples** | Octocode | GitHub-hosted projects | `githubSearchCode` |
| **Run tests** | Terminal | Verify fix | `pytest`, `npm test` |
| **Check linter errors** | Cursor Diagnostics | After editing | `read_lints` |
| **Debug execution** | Cursor Debugger | Complex logic bugs | Breakpoints + Step Through |

---

## ✅ BEST PRACTICES

### DO's ✅

1. **Always reproduce first** - Never fix bugs you can't reproduce
2. **Use version control** - Commit before attempting fixes
3. **Test thoroughly** - Unit tests + integration tests + manual testing
4. **Document root cause** - Help future maintainers
5. **Ask for help** - Pair debugging is powerful
6. **Take breaks** - Fresh eyes find bugs faster
7. **Use systematic approach** - Follow the 5-phase workflow
8. **Verify assumptions** - Don't assume, verify with evidence
9. **Check recent changes** - Often bugs come from recent commits
10. **Read error messages carefully** - They usually tell you exactly what's wrong

### DON'Ts ❌

1. **Don't guess and commit** - Always verify your fix works
2. **Don't fix symptoms** - Find and fix the root cause
3. **Don't skip testing** - Untested fixes often break other things
4. **Don't make multiple changes at once** - Change one thing, test, repeat
5. **Don't ignore warnings** - Today's warning is tomorrow's bug
6. **Don't delete error messages** - Log them for future debugging
7. **Don't rush** - Hasty fixes introduce new bugs
8. **Don't skip documentation** - Future you will thank present you
9. **Don't work in isolation** - Communicate with team
10. **Don't edit production directly** - Always test in dev/staging first

---

## 📊 DEBUGGING DECISION FLOWCHART

```
Bug Reported/Detected
    ↓
[PHASE 1: REPRODUCE]
Can reproduce? → NO → Gather more info, add logging
    ↓ YES
Have clear error message? → YES → Jump to locate
    ↓ NO
Add debug logging → Reproduce → Get error message
    ↓
[PHASE 2: LOCATE]
Error message has file:line? → YES → Go directly to location
    ↓ NO
Use Serena to search codebase → Found location?
    ↓ YES
Understand code with Serena → Know root cause?
    ↓ YES/UNCERTAIN
[PHASE 3: RESEARCH]
Search Brave/Exa/Octocode → Found solution?
    ↓ YES
Evaluate solution → Suitable?
    ↓ YES
[PHASE 4: IMPLEMENT]
Check impact with Serena → Make minimal changes → Test thoroughly
    ↓
Tests pass? → NO → Rollback, re-analyze
    ↓ YES
[PHASE 5: DOCUMENT]
Commit with good message → Update memory → Add preventive tests
    ↓
DONE ✓
```

---

## 🎓 REAL-WORLD EXAMPLE

**Scenario:** Application crashes with "TypeError: Cannot read property 'map' of undefined"

### Phase 1: Reproduce
- Run app → Navigate to /dashboard → Click "Load Data" → Crash
- Reproducible: 100% of the time
- Stack trace points to: `dashboard.js:45`

### Phase 2: Locate
```python
# Step 1: Get overview
get_symbols_overview("src/dashboard.js")
# Output: Shows loadData() function, renderChart() function

# Step 2: Find the function at line 45
find_symbol(
    name_path="renderChart",
    relative_path="src/dashboard.js",
    include_body=True
)
# Output: 
# function renderChart(data) {
#   return data.map(item => ...)  // Line 45
# }

# Step 3: Find who calls renderChart
find_referencing_symbols("renderChart", "src/dashboard.js")
# Output: Called from loadData()

# Step 4: Check loadData
find_symbol("loadData", relative_path="src/dashboard.js", include_body=True)
# Output: Calls API, then immediately calls renderChart()
# Problem: API can return null when no data exists!
```

**Root Cause Identified:** `loadData()` doesn't check if API returned null before calling `renderChart()`

### Phase 3: Research
```python
brave_web_search(query="javascript cannot read property map of undefined fix", count=5)
# Result: Multiple solutions suggest null checking

exa_get_code_context_exa(query="javascript defensive programming null check before map")
# Result: Best practice patterns for null checking
```

**Solution Found:** Add null/undefined check before calling `renderChart()`

### Phase 4: Implement
```javascript
// Before (buggy):
async function loadData() {
  const data = await fetchDataFromAPI();
  renderChart(data);  // Crashes if data is null
}

// After (fixed):
async function loadData() {
  const data = await fetchDataFromAPI();
  
  // Fix for #456: Handle null data from API
  if (!data || !Array.isArray(data)) {
    console.warn("No data received from API, showing empty state");
    showEmptyState();
    return;
  }
  
  renderChart(data);
}
```

**Testing:**
- Reproduced original crash: ✓ Now shows empty state instead
- Tested with valid data: ✓ Chart renders correctly
- Tested with empty array: ✓ Shows empty state
- Ran full test suite: ✓ All tests pass

### Phase 5: Document
```bash
git add src/dashboard.js
git commit -m "fix: handle null data from API in dashboard

Problem:
- Application crashed when API returned null/no data
- Error: 'Cannot read property map of undefined' in renderChart()

Root Cause:
- loadData() called renderChart() without checking if data exists
- API returns null when no records found

Solution:
- Added null/array check before calling renderChart()
- Show empty state instead of crashing

Tested:
- Verified crash no longer occurs with null data
- Confirmed chart still works with valid data
- All existing tests pass

Fixes #456"
```

**Prevention:**
```javascript
// Added test to prevent regression
test('loadData handles null response gracefully', async () => {
  mockAPI.fetchData.mockResolvedValue(null);
  await loadData();
  expect(showEmptyState).toHaveBeenCalled();
  expect(renderChart).not.toHaveBeenCalled();
});
```

---

## 🚨 SPECIAL CASES

### Intermittent/Flaky Bugs

**Strategy:**
1. Add extensive logging around the failure point
2. Monitor over time to find patterns (time of day, specific users, load conditions)
3. Use sequential-thinking MCP to analyze patterns
4. Hypothesis testing with each pattern found

### Performance Bugs

**Strategy:**
1. Use profiling tools (Chrome DevTools, Python cProfile)
2. Identify bottlenecks with data
3. Research optimization techniques for specific operations
4. Benchmark before and after

### Integration Bugs

**Strategy:**
1. Test each system independently
2. Use API testing tools (Postman, Insomnia)
3. Check network logs for failed requests
4. Verify configurations and environment variables
5. Check version compatibility

### Bugs Without Error Messages

**Strategy:**
1. Add logging at suspected points
2. Use binary search to narrow down
3. Compare expected vs. actual state at each step
4. Use debugger to step through execution

---

## 💡 REMEMBER

**The Golden Rule of Debugging:**
> "The bug is always in the code you're sure is correct."

**The Scientific Method:**
1. Observe (reproduce the bug)
2. Hypothesize (form theories about cause)
3. Test (verify each hypothesis)
4. Conclude (identify root cause)
5. Verify (confirm fix works)

**The Communication Rule:**
> Always explain to user:
> - What the bug was
> - Why it happened (root cause)
> - How you fixed it
> - What you did to prevent it recurring

---

## 📝 QUICK REFERENCE CHECKLIST

Before declaring "bug fixed":
- [ ] Bug is consistently reproducible
- [ ] Root cause identified (not just symptoms)
- [ ] Research completed (similar solutions reviewed)
- [ ] Minimal, focused changes made
- [ ] Original bug no longer reproduces
- [ ] Existing tests still pass
- [ ] New regression test added (if applicable)
- [ ] No new linter errors introduced
- [ ] Code documented (comments + commit message)
- [ ] Team notified (if blocking others)
- [ ] Preventive measures considered

---

**FINAL NOTE:** Debugging is a skill that improves with practice. Each bug you solve makes you a better developer. Stay systematic, stay curious, and never stop learning! 🚀