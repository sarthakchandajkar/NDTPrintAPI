const TOKEN_KEY = "ndt_settings_token";

export function getSettingsToken(): string | null {
  if (typeof window === "undefined") return null;
  return sessionStorage.getItem(TOKEN_KEY);
}

export function setSettingsToken(token: string): void {
  sessionStorage.setItem(TOKEN_KEY, token);
}

export function clearSettingsToken(): void {
  sessionStorage.removeItem(TOKEN_KEY);
}
