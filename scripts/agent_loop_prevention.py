import asyncio
import hashlib
import json
import time
import math
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ValidationError

# ---------------------------------------------------------
# Semantic Mathematics (Cosine Similarity)
# ---------------------------------------------------------
def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    """Calculates cosine similarity between two vectors."""
    dot_product = sum(x*y for x, y in zip(v1, v2))
    norm_v1 = math.sqrt(sum(x*x for x in v1))
    norm_v2 = math.sqrt(sum(x*x for x in v2))
    if norm_v1 == 0 or norm_v2 == 0:
        return 0.0
    return dot_product / (norm_v1 * norm_v2)

def get_mock_embedding(text: str) -> List[float]:
    """
    Mock embedding function. 
    In production, use `text-embedding-3-small` or local SentenceTransformers.
    """
    # Simple dummy heuristic for demonstration to simulate "near-identical" text
    if "auth" in text.lower() or "login" in text.lower():
        return [0.9, 0.8, 0.1]
    return [1.0, 0.0, 0.5]

# ---------------------------------------------------------
# Layer 1 & 3 & 4: Budget and Loop Detector Control Plane
# ---------------------------------------------------------
class AgentBudget:
    def __init__(self, max_steps: int = 8, max_cost_usd: float = 0.35, timeout_sec: float = 45.0):
        # Master Task Limits
        self.max_steps = max_steps
        self.max_cost_usd = max_cost_usd
        self.timeout_sec = timeout_sec
        
        # State Trackers
        self.start_time = time.monotonic()
        self.cumulative_cost = 0.0
        self.current_step = 0
        self.cumulative_tokens = 0
        
        # Trajectory Hash Ring Buffers
        self.hash_ring: List[str] = []
        self.embedding_ring: List[List[float]] = []

    def check_and_increment_step(self, tokens_used: int = 0, cost_usd: float = 0.0):
        """Layer 1: Hard Budget Guardrails"""
        self.current_step += 1
        self.cumulative_tokens += tokens_used
        self.cumulative_cost += cost_usd
        
        if self.current_step > self.max_steps:
            raise RuntimeError(f"BUDGET EXCEEDED: Step limit reached ({self.current_step}/{self.max_steps})")
        if self.cumulative_cost > self.max_cost_usd:
            raise RuntimeError(f"BUDGET EXCEEDED: Financial cap reached (${self.cumulative_cost:.2f})")
            
    def check_loop_hash(self, tool_name: str, args: Dict[str, Any]):
        """Layer 3: Exact Parameter Hash Ring"""
        canonical = json.dumps(args, sort_keys=True)
        fp = hashlib.sha256(f"{tool_name}:{canonical}".encode()).hexdigest()
        
        # If the same hash appears >= 2 times in the last 5 calls, abort
        if self.hash_ring[-4:].count(fp) >= 1:
            raise RuntimeError(f"LOOP DETECTED: Exact tool call repeated for '{tool_name}' with args {args}.")
        
        self.hash_ring.append(fp)
        if len(self.hash_ring) > 10:
            self.hash_ring.pop(0)

    def check_semantic_loop(self, text_query: str, threshold: float = 0.95):
        """Layer 4: Semantic Similarity Loop Guard"""
        if not text_query:
            return
            
        current_embedding = get_mock_embedding(text_query)
        
        # Check against the last 3 embeddings
        for prev_embedding in self.embedding_ring[-3:]:
            sim = cosine_similarity(current_embedding, prev_embedding)
            if sim > threshold:
                raise RuntimeError(f"SEMANTIC LOOP DETECTED: Query '{text_query}' is too similar to a previous failed query (sim: {sim:.2f}).")
                
        self.embedding_ring.append(current_embedding)
        if len(self.embedding_ring) > 10:
            self.embedding_ring.pop(0)

# ---------------------------------------------------------
# Layer 2 & 5: Tool Execution, Schema Validation & Sanitization
# ---------------------------------------------------------
class SearchQueryArgs(BaseModel):
    query: str
    limit: int = 5

async def execute_tool_sandboxed(tool_name: str, raw_args: Dict[str, Any], timeout: float = 5.0) -> str:
    """
    Executes a tool with:
    1. Pre-execution Pydantic validation
    2. Per-tool network timeouts
    3. Output sanitization / trimming
    """
    
    # Layer 2: Pre-Execution Schema Validation
    try:
        if tool_name == "search":
            validated_args = SearchQueryArgs(**raw_args)
        else:
            raise ValueError(f"Unknown tool: {tool_name}")
    except ValidationError as e:
        # Crucial: Return validation error to LLM for cognitive feedback, do NOT crash
        return f"SCHEMA VALIDATION ERROR: Please fix your JSON args. Details: {e.json()}"
        
    # Simulated External Network Call
    async def _mock_network_call():
        await asyncio.sleep(1) # Simulate 1s latency
        
        if "error 404" in validated_args.query.lower():
            # Simulate a massive unhandled stack trace return
            return "ERROR 404: Not Found\n" * 500
            
        return f"Results for '{validated_args.query}': [System Status OK, No incidents reported]"
        
    # Layer 5: Tool Timeout & Output Sanitization
    try:
        # Per-tool wall-clock timeout (e.g., 5 seconds)
        raw_output = await asyncio.wait_for(_mock_network_call(), timeout=timeout)
        
        # Output Trimmer: Prevent O(N^2) Context Bloat
        MAX_PAYLOAD_CHARS = 500
        if len(raw_output) > MAX_PAYLOAD_CHARS:
            return raw_output[:MAX_PAYLOAD_CHARS] + f"\n...[TRUNCATED {len(raw_output) - MAX_PAYLOAD_CHARS} chars]"
        return raw_output
        
    except asyncio.TimeoutError:
        return f"NETWORK ERROR: Tool '{tool_name}' timed out after {timeout}s. Downstream API unavailable."

# ---------------------------------------------------------
# The LLM Reasoner (Mock) & Master Orchestrator
# ---------------------------------------------------------
async def _agent_reasoning_loop(user_prompt: str, budget: AgentBudget):
    print(f"\n{'='*50}\n▶ Starting Agent Task: '{user_prompt}'\n{'='*50}")
    context_history = []
    
    # Mocking the LLM behavior to intentionally trigger the loop guards
    mock_llm_responses = [
        {"tool": "search", "args": {"query": "auth error 404", "limit": 10}},  # Turn 1
        {"tool": "search", "args": {"query": "auth error 404", "limit": 10}}   # Turn 2 (Exact Duplicate)
    ]
    
    for mock_llm_action in mock_llm_responses:
        # 1. Budget Guard
        budget.check_and_increment_step(tokens_used=200, cost_usd=0.003)
        print(f"\n[Step {budget.current_step}] LLM is reasoning...")
        
        tool_intent = mock_llm_action["tool"]
        llm_args = mock_llm_action["args"]
        print(f"  ↳ Proposed Action: {tool_intent}({llm_args})")
        
        # 2. Hash Ring Guard (Layer 3)
        budget.check_loop_hash(tool_intent, llm_args)
        
        # 3. Semantic Loop Guard (Layer 4)
        if "query" in llm_args:
            budget.check_semantic_loop(llm_args["query"])
            
        # 4. Sandboxed Execution (Layer 2 & 5)
        observation = await execute_tool_sandboxed(tool_intent, llm_args)
        print(f"  ↳ Observation Returned ({len(observation)} bytes):\n      {observation.replace(chr(10), ' ')}")
        
        context_history.append({"action": tool_intent, "observation": observation})
        await asyncio.sleep(0.5)

async def execute_agent_task(user_prompt: str):
    """
    Layer 1: Master Wall-Clock Orchestrator (Circuit Breaker)
    """
    budget = AgentBudget(max_steps=5, timeout_sec=15.0)
    
    try:
        # Enforce Master Wall-Clock Timer
        result = await asyncio.wait_for(
            _agent_reasoning_loop(user_prompt, budget),
            timeout=budget.timeout_sec
        )
        return {"status": "success", "result": result}
        
    except asyncio.TimeoutError:
        print("\n❌ [CIRCUIT BREAKER] Master Wall-Clock Timeout Exceeded!")
        # Rollback logic goes here
        return {"status": "aborted", "reason": "Task took too long to complete."}
        
    except RuntimeError as e:
        # Captures budget limits, hash loops, semantic loops
        print(f"\n🚨 [CIRCUIT BREAKER] Guardrail Triggered:\n    {str(e)}")
        # Rollback logic goes here
        return {"status": "aborted", "reason": str(e)}

if __name__ == "__main__":
    # Execute the mock workflow
    asyncio.run(execute_agent_task("Investigate why the server is returning 500 errors"))
