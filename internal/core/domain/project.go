package domain

import (
	"strings"
)

// ProjectCredential represents authentication credentials and authorization scopes for a tenant/project.
type ProjectCredential struct {
	ProjectID     string   `json:"project_id"`
	Name          string   `json:"name"`
	APIKey        string   `json:"api_key"`
	JWTSecret     string   `json:"jwt_secret"`
	AllowedScopes []string `json:"allowed_scopes"` // e.g. ["user", "project_global"] or ["*"]
	Enabled       bool     `json:"enabled"`
}

// HasScope checks if the credential is authorized to broadcast at the requested scope level.
// Wildcard "*" grants permission for all scope levels (including system_global).
func (p ProjectCredential) HasScope(scope Scope) bool {
	targetScope := string(scope)
	for _, s := range p.AllowedScopes {
		trimmed := strings.TrimSpace(s)
		if trimmed == "*" || strings.EqualFold(trimmed, targetScope) {
			return true
		}
	}
	return false
}

// CanPublishFor checks if the credential is authorized to publish notifications for targetProjectID.
// System or wildcard credentials can publish for any project. Standard tenants can only publish for themselves.
func (p ProjectCredential) CanPublishFor(targetProjectID string) bool {
	if p.ProjectID == "system" || p.ProjectID == "*" {
		return true
	}
	return strings.EqualFold(p.ProjectID, targetProjectID)
}

