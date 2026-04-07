import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { GoogleLogin, GoogleOAuthProvider } from '@react-oauth/google';
import { useAuth } from '../contexts/AuthContext';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Car, Warehouse } from '@phosphor-icons/react';

// REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH

function LoginContent() {
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [registerName, setRegisterName] = useState('');
  const [registerEmail, setRegisterEmail] = useState('');
  const [registerPassword, setRegisterPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login, register, googleLogin } = useAuth();
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await login(loginEmail, loginPassword);
      navigate('/');
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Credenciales inválidas');
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await register(registerName, registerEmail, registerPassword);
      navigate('/');
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Error al registrar');
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
              <CardDescription>Ingresa a tu cuenta o regístrate</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs defaultValue="login" className="w-full">
                <TabsList className="grid w-full grid-cols-2 mb-4">
                  <TabsTrigger value="login" data-testid="login-tab">Iniciar Sesión</TabsTrigger>
                  <TabsTrigger value="register" data-testid="register-tab">Registrarse</TabsTrigger>
                </TabsList>

                <TabsContent value="login">
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
                      <Input
                        id="login-password"
                        type="password"
                        value={loginPassword}
                        onChange={(e) => setLoginPassword(e.target.value)}
                        required
                        data-testid="login-password-input"
                      />
                    </div>
                    {error && (
                      <p className="text-sm text-destructive" data-testid="login-error">{error}</p>
                    )}
                    <Button
                      type="submit"
                      className="w-full bg-[#002FA7] hover:bg-[#002FA7]/90"
                      disabled={loading}
                      data-testid="login-submit-btn"
                    >
                      {loading ? 'Ingresando...' : 'Iniciar Sesión'}
                    </Button>
                  </form>
                </TabsContent>

                <TabsContent value="register">
                  <form onSubmit={handleRegister} className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="register-name">Nombre</Label>
                      <Input
                        id="register-name"
                        type="text"
                        placeholder="Tu nombre"
                        value={registerName}
                        onChange={(e) => setRegisterName(e.target.value)}
                        required
                        data-testid="register-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="register-email">Email</Label>
                      <Input
                        id="register-email"
                        type="email"
                        placeholder="tu@email.com"
                        value={registerEmail}
                        onChange={(e) => setRegisterEmail(e.target.value)}
                        required
                        data-testid="register-email-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="register-password">Contraseña</Label>
                      <Input
                        id="register-password"
                        type="password"
                        value={registerPassword}
                        onChange={(e) => setRegisterPassword(e.target.value)}
                        required
                        data-testid="register-password-input"
                      />
                    </div>
                    {error && (
                      <p className="text-sm text-destructive" data-testid="register-error">{error}</p>
                    )}
                    <Button
                      type="submit"
                      className="w-full bg-[#002FA7] hover:bg-[#002FA7]/90"
                      disabled={loading}
                      data-testid="register-submit-btn"
                    >
                      {loading ? 'Registrando...' : 'Crear Cuenta'}
                    </Button>
                  </form>
                </TabsContent>
              </Tabs>

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
