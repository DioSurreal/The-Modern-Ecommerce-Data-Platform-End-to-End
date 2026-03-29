variable "snowflake_username" {
  type        = string
  description = "Snowflake username"
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password"
  sensitive   = true # ใส่ตัวนี้ไว้ เวลา terraform plan มันจะได้ไม่โชว์รหัสผ่านบนจอ
}