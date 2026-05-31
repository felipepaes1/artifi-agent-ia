import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/NotFound";
import { Route, Switch } from "wouter";
import ErrorBoundary from "./components/ErrorBoundary";
import { ThemeProvider } from "./contexts/ThemeContext";
import { TenantProvider } from "./contexts/TenantContext";
import DashboardLayout from "./components/DashboardLayout";
import Dashboard from "./pages/Dashboard";
import Conversations from "./pages/Conversations";
import Appointments from "./pages/Appointments";
import Performance from "./pages/Performance";

function Router() {
  return (
    <DashboardLayout>
      <Switch>
        <Route path="/" component={Dashboard} />
        <Route path="/conversas" component={Conversations} />
        <Route path="/agendamentos" component={Appointments} />
        <Route path="/desempenho" component={Performance} />
        <Route path="/404" component={NotFound} />
        <Route component={NotFound} />
      </Switch>
    </DashboardLayout>
  );
}

function App() {
  return (
    <ErrorBoundary>
      <ThemeProvider defaultTheme="dark" switchable>
        <TenantProvider>
          <TooltipProvider>
            <Toaster />
            <Router />
          </TooltipProvider>
        </TenantProvider>
      </ThemeProvider>
    </ErrorBoundary>
  );
}

export default App;
