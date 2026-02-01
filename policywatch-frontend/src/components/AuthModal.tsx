// src/components/AuthModal.tsx
import React, { useMemo, useState } from "react";
import { supabase } from "../supabaseClient";

type Mode = "login" | "signup" | "forgot" | "updatePassword";

type Props = {
  open: boolean;
  onClose: () => void;
  initialMode?: Mode;
};

export const AuthModal: React.FC<Props> = ({ open, onClose, initialMode = "login" }) => {
  const [mode, setMode] = useState<Mode>(initialMode);

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  // signup fields
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");

  // reset password fields
  const [newPassword, setNewPassword] = useState("");
  const [confirmNewPassword, setConfirmNewPassword] = useState("");

  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const siteUrl = useMemo(() => {
    return (import.meta.env.VITE_SITE_URL as string) || window.location.origin;
  }, []);

  // keep mode in sync when parent changes
  React.useEffect(() => {
    if (open) {
      setMode(initialMode);
      setMsg(null);
      setErr(null);
    }
  }, [open, initialMode]);

  if (!open) return null;

  const resetNotices = () => {
    setMsg(null);
    setErr(null);
  };

  const close = () => {
    if (busy) return;
    onClose();
  };

  const signInWithGoogle = async () => {
    resetNotices();
    setBusy(true);
    try {
      const { error } = await supabase.auth.signInWithOAuth({
        provider: "google",
        options: {
          redirectTo: siteUrl, // returns back to your SPA
        },
      });
      if (error) throw error;
      // supabase will redirect, so no further action here
    } catch (e: any) {
      setErr(e?.message ?? "Google sign-in failed.");
    } finally {
      setBusy(false);
    }
  };

  const login = async () => {
    resetNotices();
    setBusy(true);
    try {
      const { error } = await supabase.auth.signInWithPassword({
        email: email.trim(),
        password,
      });
      if (error) throw error;

      setMsg("Logged in.");
      onClose();
    } catch (e: any) {
      setErr(e?.message ?? "Login failed.");
    } finally {
      setBusy(false);
    }
  };

  const signup = async () => {
    resetNotices();
    setBusy(true);
    try {
      const { error } = await supabase.auth.signUp({
        email: email.trim(),
        password,
        options: {
          emailRedirectTo: siteUrl, // email verification link returns here
          data: {
            first_name: firstName.trim(),
            last_name: lastName.trim(),
            full_name: `${firstName.trim()} ${lastName.trim()}`.trim(),
          },
        },
      });
      if (error) throw error;

      setMsg("Check your email to verify your account, then come back and log in.");
    } catch (e: any) {
      setErr(e?.message ?? "Sign up failed.");
    } finally {
      setBusy(false);
    }
  };

  const sendResetEmail = async () => {
    resetNotices();
    setBusy(true);
    try {
      const redirectTo = `${siteUrl}`; // same SPA; PASSWORD_RECOVERY event will trigger in App.tsx
      const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), { redirectTo });
      if (error) throw error;

      setMsg("Password reset email sent. Open the link, then set a new password.");
    } catch (e: any) {
      setErr(e?.message ?? "Failed to send reset email.");
    } finally {
      setBusy(false);
    }
  };

  const updatePassword = async () => {
    resetNotices();

    if (!newPassword || newPassword.length < 8) {
      setErr("New password must be at least 8 characters.");
      return;
    }
    if (newPassword !== confirmNewPassword) {
      setErr("Passwords do not match.");
      return;
    }

    setBusy(true);
    try {
      const { error } = await supabase.auth.updateUser({ password: newPassword });
      if (error) throw error;

      setMsg("Password updated. You’re now signed in.");
      onClose();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to update password.");
    } finally {
      setBusy(false);
    }
  };

  const title =
    mode === "login"
      ? "Log in"
      : mode === "signup"
      ? "Create account"
      : mode === "forgot"
      ? "Reset password"
      : "Set new password";

  return (
    <div className="authOverlay" role="dialog" aria-modal="true" onMouseDown={close}>
      <div className="authModal" onMouseDown={(e) => e.stopPropagation()}>
        <div className="authModal__top">
          <div className="authModal__title">{title}</div>
          <button className="authModal__close" onClick={close} aria-label="Close">
            ×
          </button>
        </div>

        {(msg || err) && (
          <div className={`authModal__notice ${err ? "is-error" : "is-ok"}`}>
            {err ? err : msg}
          </div>
        )}

        {mode !== "updatePassword" && (
          <button className="authBtn authBtn--google" onClick={signInWithGoogle} disabled={busy}>
            <span className="authBtn__icon" aria-hidden="true">
              {/* Google "G" (simple, clean, theme-friendly) */}
              <svg viewBox="0 0 48 48" width="18" height="18">
                <path
                  fill="currentColor"
                  opacity="0.95"
                  d="M24 22.5v6.9h9.8c-.4 2.2-2.6 6.4-9.8 6.4-5.9 0-10.7-4.9-10.7-10.9S18.1 14 24 14c3.4 0 5.6 1.4 6.9 2.6l4.7-4.5C32.8 9.6 28.8 7.5 24 7.5 14.9 7.5 7.5 15 7.5 24S14.9 40.5 24 40.5c10.7 0 17.8-7.5 17.8-18 0-1.2-.1-2-.3-2.9H24z"
                />
              </svg>
            </span>
            <span>Continue with Google</span>
          </button>
        )}

        {mode !== "updatePassword" && (
        <div className="authDivider" role="presentation">
            <span className="authDivider__line" />
            <span className="authDivider__text">or</span>
            <span className="authDivider__line" />
        </div>
        )}

        {mode === "signup" && (
          <div className="authGrid2">
            <label className="authField">
              <span className="authLabel">First name</span>
              <input
                className="authInput"
                value={firstName}
                onChange={(e) => setFirstName(e.target.value)}
                placeholder="Saptorshee"
                autoComplete="given-name"
              />
            </label>

            <label className="authField">
              <span className="authLabel">Last name</span>
              <input
                className="authInput"
                value={lastName}
                onChange={(e) => setLastName(e.target.value)}
                placeholder="Nag"
                autoComplete="family-name"
              />
            </label>
          </div>
        )}

        {mode !== "updatePassword" && (
          <>
            <label className="authField">
              <span className="authLabel">Email</span>
              <input
                className="authInput"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@email.com"
                autoComplete="email"
              />
            </label>

            {mode !== "forgot" && (
              <label className="authField">
                <span className="authLabel">Password</span>
                <input
                  className="authInput"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  autoComplete={mode === "login" ? "current-password" : "new-password"}
                />
              </label>
            )}
          </>
        )}

        {mode === "updatePassword" && (
          <>
            <label className="authField">
              <span className="authLabel">New password</span>
              <input
                className="authInput"
                type="password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="At least 8 characters"
                autoComplete="new-password"
              />
            </label>

            <label className="authField">
              <span className="authLabel">Confirm new password</span>
              <input
                className="authInput"
                type="password"
                value={confirmNewPassword}
                onChange={(e) => setConfirmNewPassword(e.target.value)}
                placeholder="Repeat password"
                autoComplete="new-password"
              />
            </label>
          </>
        )}

        <div className="authModal__actions">
          {mode === "login" && (
            <>
              <button className="authBtn" onClick={login} disabled={busy}>
                Log in
              </button>

              <div className="authLinks">
                <button className="authLink" onClick={() => (resetNotices(), setMode("forgot"))} disabled={busy}>
                  Forgot password?
                </button>
                <button className="authLink" onClick={() => (resetNotices(), setMode("signup"))} disabled={busy}>
                  Create account
                </button>
              </div>
            </>
          )}

          {mode === "signup" && (
            <>
              <button className="authBtn" onClick={signup} disabled={busy}>
                Sign up
              </button>

              <div className="authLinks">
                <button className="authLink" onClick={() => (resetNotices(), setMode("login"))} disabled={busy}>
                  Already have an account? Log in
                </button>
              </div>
            </>
          )}

          {mode === "forgot" && (
            <>
              <button className="authBtn" onClick={sendResetEmail} disabled={busy}>
                Send reset link
              </button>

              <div className="authLinks">
                <button className="authLink" onClick={() => (resetNotices(), setMode("login"))} disabled={busy}>
                  Back to login
                </button>
              </div>
            </>
          )}

          {mode === "updatePassword" && (
            <>
              <button className="authBtn" onClick={updatePassword} disabled={busy}>
                Update password
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
};
