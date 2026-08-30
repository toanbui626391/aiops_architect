import asyncio
import uuid
import json
import logging
from typing import Dict, Any, List

# In a production environment, you would use:
# import httpx 
# from pydantic import BaseModel, ValidationError

# ---------------------------------------------------------
# 0. Setup Mock Classes (For Standalone Execution)
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("AgentOrchestrator")

# Mocking Pydantic & HTTPX to allow this script to run anywhere without pip installs
class ValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        super().__init__(str(errors))

class RefundRequest:
    def __init__(self, **kwargs):
        if not isinstance(kwargs.get("amount"), (int, float)) or kwargs.get("amount") <= 0:
            raise ValidationError([{"loc": ["amount"], "msg": "Amount must be a positive number"}])
        
        # Simulating a UUID requirement for user_id
        try:
            uuid.UUID(str(kwargs.get("user_id")))
        except ValueError:
            raise ValidationError([{"loc": ["user_id"], "msg": "user_id must be a valid UUID"}])
            
        self.user_id = kwargs["user_id"]
        self.amount = kwargs["amount"]
        
class MockHTTPStatusError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


# ---------------------------------------------------------
# 1. Durable State Interface (The Checkpointer)
# ---------------------------------------------------------
class DatabaseCheckpointer:
    """
    Simulates a PostgreSQL / Redis durable state store.
    This entirely decouples the Agent's memory from the local Python process.
    """
    def __init__(self):
        self._db: Dict[str, Dict[str, Any]] = {}

    def save_checkpoint(self, run_id: str, step: int, context: List[Dict[str, Any]]):
        self._db[run_id] = {"step": step, "context": context}
        logger.info(f"💾 [DB CHECKPOINT] Saved Run '{run_id}' at Step {step}")

    def load_checkpoint(self, run_id: str) -> Dict[str, Any]:
        state = self._db.get(run_id)
        if state:
            logger.info(f"🔄 [DB RECOVERY] Hydrated Run '{run_id}' from Step {state['step']}")
            return state
        return {"step": 0, "context": []}
        
    def simulate_server_crash(self):
        logger.warning("🔥 [CRITICAL] SIMULATING KUBERNETES POD CRASH / OOM KILL! 🔥")
        # In a real crash, the python process dies entirely. 
        # But our state is safe in self._db!


# ---------------------------------------------------------
# 2. Tool Execution with Idempotency & Error Routing
# ---------------------------------------------------------
async def execute_refund_tool(args: Dict[str, Any], idempotency_key: str) -> str:
    """
    Bifurcated error routing:
    - Invalid Schema -> Return feedback to LLM
    - Network 503 -> Silent transport backoff retry
    """
    
    # ROUTE 1: Cognitive Schema Error Interception
    try:
        valid_args = RefundRequest(**args)
    except ValidationError as e:
        logger.error(f"❌ [COGNITIVE ERROR] Schema invalid. Rejecting before network call.")
        # Pass feedback directly to LLM context
        return f"SCHEMA ERROR: {e.errors}. Please correct your arguments."

    # ROUTE 2: Network Execution & Infra Error Handling
    headers = {"Idempotency-Key": idempotency_key}
    
    # Exponential backoff loop (Transport Layer)
    for attempt in range(1, 4):
        try:
            logger.info(f"🌐 [NETWORK] Issuing Refund with Key {idempotency_key} (Attempt {attempt})")
            
            # --- Simulating Network Dynamics ---
            await asyncio.sleep(0.5) 
            
            if attempt == 1:
                # Simulate a random 503 API Timeout on the first try
                raise MockHTTPStatusError(503, "Service Temporarily Unavailable")
            
            # Simulate the Idempotency Cache working on the downstream API
            # If the API sees the same idempotency key twice, it doesn't double-charge.
            return f"SUCCESS: Refund processed. (Idempotency Key: {idempotency_key})"
            
        except MockHTTPStatusError as e:
            if e.status_code in [500, 502, 503, 504]:
                # Infrastructure error: Silent retry with backoff
                wait_time = 2 ** attempt
                logger.warning(f"⚠️ [INFRA ERROR] {e.status_code} {e.message} -> Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time) 
                continue
            else:
                # Other HTTP 4xx (Authentication, etc)
                return f"API ERROR: {e.message}"
                
    # If all infra retries fail
    return "SYSTEM ERROR: Downstream refund API is currently unavailable. Please notify the user."


# ---------------------------------------------------------
# 3. Durable Workflow Execution
# ---------------------------------------------------------
async def run_durable_agent(run_id: str, db: DatabaseCheckpointer, crash_on_step: int = -1):
    # 1. Hydrate state from DB (Handles Server Crash Recovery automatically)
    state = db.load_checkpoint(run_id)
    current_step = state["step"]
    context_history = state["context"]
    
    logger.info(f"▶️ Starting Agent Execution Loop...")
    
    while current_step < 3:
        current_step += 1
        logger.info(f"--- Step {current_step} ---")
        
        # Generate Deterministic Idempotency Key based on Step + Run ID
        # Even if the agent crashes, when it repeats this step, the UUID5 is mathematically identical!
        step_idempotency_key = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{run_id}_step_{current_step}"))
        
        # Mock LLM Action
        if current_step == 1:
            # Simulate the LLM hallucinating a string instead of a UUID
            logger.info("🤖 [LLM] Proposing action: Refund {user_id: 'john_doe', amount: 50.0}")
            llm_action = {"user_id": "john_doe", "amount": 50.0} 
        else:
            # Simulate the LLM correcting its mistake after getting feedback
            logger.info("🤖 [LLM] Proposing action: Refund {user_id: '123e4567-e89b-12d3-a456-426614174000', amount: 50.0}")
            llm_action = {"user_id": "123e4567-e89b-12d3-a456-426614174000", "amount": 50.0}
            
        # Execute Tool (Intercepts errors, performs safe retries)
        observation = await execute_refund_tool(llm_action, step_idempotency_key)
        
        logger.info(f"🔎 [OBSERVATION] {observation}")
        context_history.append({"step": current_step, "obs": observation})
        
        # 2. Commit Checkpoint to DB (Safely locking in progress)
        db.save_checkpoint(run_id, current_step, context_history)
        
        # --- CRASH SIMULATION ---
        if current_step == crash_on_step:
            db.simulate_server_crash()
            return  # Hard exit from the function (simulating process death)
            
        if "SUCCESS" in observation:
            logger.info("✅ Task completed successfully. Exiting workflow.")
            break
            
        await asyncio.sleep(1)

# ---------------------------------------------------------
# 4. Crash Recovery Simulation
# ---------------------------------------------------------
async def main():
    db_instance = DatabaseCheckpointer()
    run_id = "run_prod_999"
    
    print("\n" + "="*60)
    print("🎬 ATTEMPT 1: Simulating an agent that crashes midway...")
    print("="*60)
    
    # Run the agent, but intentionally trigger a simulated crash on Step 1
    await run_durable_agent(run_id, db_instance, crash_on_step=1)
    
    print("\n" + "="*60)
    print("🎬 ATTEMPT 2: New Pod Spins Up. Resuming execution...")
    print("="*60)
    
    # A new server process starts, receives the same run_id from a queue, and resumes
    await run_durable_agent(run_id, db_instance)

if __name__ == "__main__":
    asyncio.run(main())
