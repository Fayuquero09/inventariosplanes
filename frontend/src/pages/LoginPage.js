import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { GoogleLogin, GoogleOAuthProvider } from '@react-oauth/google';
import { useAuth } from '../contexts/AuthContext';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { Car, Eye, EyeSlash } from '@phosphor-icons/react';

// REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH

function LoginContent() {
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [showLoginPassword, setShowLoginPassword] = useState(false);
  const [showResetPassword, setShowResetPassword] = useState(false);
  const [showResetSection, setShowResetSection] = useState(false);
  const [resetEmail, setResetEmail] = useState('');
  const [resetPassword, setResetPassword] = useState('');
  const [resetConfirmPassword, setResetConfirmPassword] = useState('');
  const [resetMessage, setResetMessage] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login, googleLogin, resetPassword: resetPasswordAction } = useAuth();
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await login((loginEmail || '').trim(), loginPassword);
      navigate('/');
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Credenciales inválidas');
    } finally {
      setLoading(false);
    }
  };

  const handleGoogleSuccess = async (response) => {
    setError('');
    setLoading(true);
    try {
      await googleLogin(response.credential);
      navigate('/');
    } catch (err) {
      setError('Error al iniciar sesión con Google');
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async (e) => {
    e.preventDefault();
    setError('');
    setResetMessage('');

    if (resetPassword.length < 8) {
      setError('La nueva contraseña debe tener al menos 8 caracteres');
      return;
    }

    if (resetPassword !== resetConfirmPassword) {
      setError('Las contraseñas no coinciden');
      return;
    }

    setLoading(true);
    try {
      await resetPasswordAction((resetEmail || '').trim(), resetPassword);
      setResetMessage('Contraseña actualizada. Ya puedes iniciar sesión.');
      setResetPassword('');
      setResetConfirmPassword('');
      setShowResetSection(false);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'No se pudo resetear la contraseña');
    } finally {
      setLoading(false);
    }
  };

  const googleClientId = process.env.REACT_APP_GOOGLE_CLIENT_ID;

  return (
    <div className="min-h-screen flex">
      {/* Left side - Background image */}
      <div className="hidden lg:flex lg:w-1/2 login-bg relative">
        <div className="absolute inset-0 bg-black/60" />
        <div className="relative z-10 flex flex-col justify-center px-12 text-white">
          <div className="flex items-center gap-3 mb-8">
            <div className="w-12 h-12 rounded-md bg-[#002FA7] flex items-center justify-center">
              <Car size={28} weight="duotone" />
            </div>
            <span className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>AutoConnect</span>
          </div>
          <h1 className="text-4xl lg:text-5xl font-bold leading-tight mb-4" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Gestión Inteligente de Inventario Vehicular
          </h1>
          <p className="text-lg text-white/80">
            Controla inventarios, calcula costos financieros, gestiona comisiones y optimiza ventas en todas tus agencias.
          </p>
        </div>
      </div>

      {/* Right side - Login form */}
      <div className="flex-1 flex items-center justify-center p-8 bg-background">
        <div className="w-full max-w-md">
          {/* Mobile logo */}
          <div className="flex items-center gap-3 mb-8 lg:hidden">
            <div className="w-10 h-10 rounded-md bg-[#002FA7] flex items-center justify-center">
              <Car size={24} weight="duotone" className="text-white" />
            </div>
            <span className="text-xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>AutoConnect</span>
          </div>

          <Card className="border-border/40">
            <CardHeader className="space-y-1">
              <CardTitle className="text-2xl" style={{ fontFamily: 'Cabinet Grotesk' }}>Bienvenido</CardTitle>
              <CardDescription>Ingresa a tu cuenta</CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleLogin} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="login-email">Email</Label>
                  <Input
                    id="login-email"
                    type="email"
                    placeholder="tu@email.com"
                    value={loginEmail}
                    onChange={(e) => setLoginEmail(e.target.value)}
                    required
                    data-testid="login-email-input"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="login-password">Contraseña</Label>
                  <div className="relative">
                    <Input
                      id="login-password"
                      type={showLoginPassword ? 'text' : 'password'}
                      value={loginPassword}
                      onChange={(e) => setLoginPassword(e.target.value)}
                      required
                      className="pr-10"
                      data-testid="login-password-input"
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="absolute inset-y-0 right-0 h-full px-3"
                      onClick={() => setShowLoginPassword((prev) => !prev)}
                      data-testid="toggle-login-password-btn"
                    >
                      {showLoginPassword ? <EyeSlash size={18} /> : <Eye size={18} />}
                    </Button>
                  </div>
                </div>
                {error && (
                  <p className="text-sm text-destructive" data-testid="login-error">{error}</p>
                )}
                {resetMessage && (
                  <p className="text-sm text-[#2A9D8F]" data-testid="reset-success-message">{resetMessage}</p>
                )}
                <Button
                  type="submit"
                  className="w-full bg-[#002FA7] hover:bg-[#002FA7]/90"
                  disabled={loading}
                  data-testid="login-submit-btn"
                >
                  {loading ? 'Ingresando...' : 'Iniciar Sesión'}
                </Button>

                <Button
                  type="button"
                  variant="link"
                  className="px-0 text-sm"
                  onClick={() => {
                    setError('');
                    setResetMessage('');
                    setShowResetSection((prev) => !prev);
                  }}
                  data-testid="show-reset-password-btn"
                >
                  {showResetSection ? 'Cancelar reseteo de contraseña' : '¿Olvidaste tu contraseña?'}
                </Button>

                {showResetSection && (
                  <div className="rounded-md border border-border/50 p-3 space-y-3">
                    <div className="space-y-2">
                      <Label htmlFor="reset-email">Email de la cuenta</Label>
                      <Input
                        id="reset-email"
                        type="email"
                        value={resetEmail}
                        onChange={(e) => setResetEmail(e.target.value)}
                        required
                        data-testid="reset-email-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="reset-password">Nueva contraseña</Label>
                      <div className="relative">
                        <Input
                          id="reset-password"
                          type={showResetPassword ? 'text' : 'password'}
                          value={resetPassword}
                          onChange={(e) => setResetPassword(e.target.value)}
                          required
                          className="pr-10"
                          data-testid="reset-password-input"
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute inset-y-0 right-0 h-full px-3"
                          onClick={() => setShowResetPassword((prev) => !prev)}
                          data-testid="toggle-reset-password-btn"
                        >
                          {showResetPassword ? <EyeSlash size={18} /> : <Eye size={18} />}
                        </Button>
                      </div>
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="reset-confirm-password">Confirmar nueva contraseña</Label>
                      <Input
                        id="reset-confirm-password"
                        type={showResetPassword ? 'text' : 'password'}
                        value={resetConfirmPassword}
                        onChange={(e) => setResetConfirmPassword(e.target.value)}
                        required
                        data-testid="reset-confirm-password-input"
                      />
                    </div>
                    <Button
                      type="button"
                      className="w-full"
                      disabled={loading}
                      onClick={handleResetPassword}
                      data-testid="reset-password-submit-btn"
                    >
                      {loading ? 'Actualizando...' : 'Resetear contraseña'}
                    </Button>
                  </div>
                )}
              </form>

              {googleClientId && (
                <>
                  <div className="relative my-6">
                    <div className="absolute inset-0 flex items-center">
                      <div className="w-full border-t border-border/40" />
                    </div>
                    <div className="relative flex justify-center text-xs uppercase">
                      <span className="bg-card px-2 text-muted-foreground">O continúa con</span>
                    </div>
                  </div>

                  <div className="flex justify-center" data-testid="google-login-container">
                    <GoogleLogin
                      onSuccess={handleGoogleSuccess}
                      onError={() => setError('Error al iniciar sesión con Google')}
                      useOneTap
                      theme="outline"
                      size="large"
                      text="continue_with"
                      shape="rectangular"
                    />
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

export default function LoginPage() {
  const googleClientId = process.env.REACT_APP_GOOGLE_CLIENT_ID;

  if (googleClientId) {
    return (
      <GoogleOAuthProvider clientId={googleClientId}>
        <LoginContent />
      </GoogleOAuthProvider>
    );
  }

  return <LoginContent />;
}
