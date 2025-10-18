from __future__ import annotations


class HealthService:    
    async def get_health_status(self) -> dict[str, str]:
        """Simple healthcheck endpoint."""
        return {"status": "healthy"} 