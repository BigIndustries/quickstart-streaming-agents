variable "mcp_backend" {
  description = "Remote MCP server backend: 'lambda' (Confluent-hosted), 'bigind' (Big Industries managed), or 'zapier'"
  type        = string
  default     = "lambda"

  validation {
    condition     = contains(["lambda", "bigind", "zapier"], var.mcp_backend)
    error_message = "mcp_backend must be 'lambda', 'bigind', or 'zapier'."
  }
}

variable "mcp_token" {
  description = "Bearer token for the Confluent-hosted remote MCP server"
  type        = string
  default     = ""
  sensitive   = true
}

variable "bigind_mcp_endpoint" {
  description = "URL of the Big Industries managed MCP server"
  type        = string
  default     = ""
  sensitive   = true
}

variable "bigind_mcp_token" {
  description = "Bearer token for the Big Industries managed MCP server"
  type        = string
  default     = ""
  sensitive   = true
}

variable "zapier_token" {
  description = "Bearer token for the Zapier Remote MCP server"
  type        = string
  default     = ""
  sensitive   = true
}
