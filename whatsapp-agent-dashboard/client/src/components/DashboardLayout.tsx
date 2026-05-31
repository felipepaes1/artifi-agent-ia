// ============================================================
// DESIGN: "Pulse" — Dark Command Center Layout
// Sidebar fixa com ícones + labels, header com filtros globais
// Mobile: sidebar oculta com menu hamburger
// ============================================================

import { useState, useEffect } from "react";
import { useLocation, Link } from "wouter";
import {
  LayoutDashboard,
  MessageSquare,
  CalendarCheck,
  Activity,
  ChevronLeft,
  ChevronRight,
  Bot,
  Bell,
  Settings,
  Menu,
  X,
  Sun,
  Moon,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { toast } from "sonner";
import { RANGE_OPTIONS, useTenant } from "@/contexts/TenantContext";
import { useTheme } from "@/contexts/ThemeContext";

const RANGE_LABELS: Record<number, string> = {
  7: "Últimos 7 dias",
  30: "Últimos 30 dias",
  90: "Últimos 90 dias",
};

const AVATAR_URL =
  "https://d2xsxph8kpxj0f.cloudfront.net/310519663114833595/XKGboP74ak8U9ZQCLu7t87/whatsapp-agent-avatar-KgkaHVSz8SKHHtF3TMoL8z.webp";

interface NavItem {
  label: string;
  icon: React.ElementType;
  path: string;
}

const navItems: NavItem[] = [
  { label: "Visão Geral", icon: LayoutDashboard, path: "/" },
  { label: "Conversas", icon: MessageSquare, path: "/conversas" },
  { label: "Agendamentos", icon: CalendarCheck, path: "/agendamentos" },
  { label: "Desempenho", icon: Activity, path: "/desempenho" },
];

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [location] = useLocation();
  const { tenants, slug, setSlug, range, setRange } = useTenant();
  const { theme, toggleTheme } = useTheme();

  // Close mobile menu on route change
  useEffect(() => {
    setMobileOpen(false);
  }, [location]);

  // Close mobile menu on resize to desktop
  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) setMobileOpen(false);
    };
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const SidebarContent = ({ isMobile = false }: { isMobile?: boolean }) => (
    <>
      {/* Logo area */}
      <div className="flex items-center gap-3 px-4 h-16 border-b border-border">
        <div className="relative shrink-0">
          <img
            src={AVATAR_URL}
            alt="Agent"
            className="w-9 h-9 rounded-lg object-cover"
          />
          <span className="absolute -bottom-0.5 -right-0.5 w-3 h-3 bg-emerald-500 rounded-full border-2 border-sidebar animate-pulse-dot" />
        </div>
        {(isMobile || !collapsed) && (
          <div className="animate-slide-in-left overflow-hidden flex-1">
            <p className="font-display font-semibold text-sm text-sidebar-foreground leading-tight truncate">
              Agente WhatsApp
            </p>
            <p className="text-[11px] text-muted-foreground leading-tight">
              Online agora
            </p>
          </div>
        )}
        {isMobile && (
          <button
            onClick={() => setMobileOpen(false)}
            className="p-1 text-muted-foreground hover:text-foreground"
          >
            <X size={20} />
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4 px-2 space-y-1">
        {navItems.map((item) => {
          const isActive = location === item.path;
          const Icon = item.icon;
          const showLabel = isMobile || !collapsed;
          return (
            <Tooltip
              key={item.path}
              delayDuration={!showLabel ? 100 : 1000}
            >
              <TooltipTrigger asChild>
                <Link href={item.path}>
                  <div
                    className={`flex items-center gap-3 px-3 py-2.5 rounded-md transition-all duration-200 group ${
                      isActive
                        ? "bg-primary/10 text-primary"
                        : "text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent"
                    }`}
                  >
                    <Icon
                      className={`shrink-0 transition-all duration-200 ${
                        isActive
                          ? "text-primary drop-shadow-[0_0_6px_oklch(0.82_0.15_195/40%)]"
                          : "group-hover:text-sidebar-foreground"
                      }`}
                      size={20}
                    />
                    {showLabel && (
                      <span className="text-sm font-medium truncate animate-slide-in-left">
                        {item.label}
                      </span>
                    )}
                    {isActive && showLabel && (
                      <div className="ml-auto w-1.5 h-1.5 rounded-full bg-primary animate-pulse-dot" />
                    )}
                  </div>
                </Link>
              </TooltipTrigger>
              {!showLabel && (
                <TooltipContent side="right" sideOffset={8}>
                  {item.label}
                </TooltipContent>
              )}
            </Tooltip>
          );
        })}
      </nav>

      {/* Bottom section */}
      <div className="px-2 pb-4 space-y-1">
        <Tooltip delayDuration={(!isMobile && collapsed) ? 100 : 1000}>
          <TooltipTrigger asChild>
            <button
              onClick={() => toast("Configurações em breve")}
              className="flex items-center gap-3 px-3 py-2.5 rounded-md text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent transition-all duration-200 w-full"
            >
              <Settings size={20} className="shrink-0" />
              {(isMobile || !collapsed) && (
                <span className="text-sm font-medium truncate">
                  Configurações
                </span>
              )}
            </button>
          </TooltipTrigger>
          {!isMobile && collapsed && (
            <TooltipContent side="right" sideOffset={8}>
              Configurações
            </TooltipContent>
          )}
        </Tooltip>
      </div>
    </>
  );

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      {/* Desktop Sidebar */}
      <aside
        className={`relative hidden lg:flex flex-col border-r border-border bg-sidebar transition-all duration-300 ease-out ${
          collapsed ? "w-[68px]" : "w-[240px]"
        }`}
      >
        <SidebarContent />
        {/* Collapse toggle */}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="absolute -right-3 top-20 w-6 h-6 rounded-full bg-secondary border border-border flex items-center justify-center text-muted-foreground hover:text-foreground hover:bg-accent transition-all duration-200 z-10"
        >
          {collapsed ? (
            <ChevronRight size={14} />
          ) : (
            <ChevronLeft size={14} />
          )}
        </button>
      </aside>

      {/* Mobile Sidebar Overlay */}
      {mobileOpen && (
        <div
          className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40 lg:hidden"
          onClick={() => setMobileOpen(false)}
        />
      )}

      {/* Mobile Sidebar */}
      <aside
        className={`fixed top-0 left-0 h-full w-[260px] bg-sidebar border-r border-border z-50 flex flex-col transition-transform duration-300 ease-out lg:hidden ${
          mobileOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <SidebarContent isMobile />
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <header className="h-14 lg:h-16 border-b border-border bg-sidebar/50 backdrop-blur-sm flex items-center justify-between px-4 lg:px-6 shrink-0">
          <div className="flex items-center gap-3">
            {/* Mobile menu button */}
            <button
              onClick={() => setMobileOpen(true)}
              className="lg:hidden p-1.5 text-muted-foreground hover:text-foreground rounded-md hover:bg-secondary/50 transition-colors"
            >
              <Menu size={20} />
            </button>

            {/* Seletor de cliente (tenant) */}
            <Select value={slug} onValueChange={setSlug}>
              <SelectTrigger className="w-40 sm:w-52 h-9 bg-secondary/50 border-border/50 text-sm">
                <SelectValue placeholder="Selecionar cliente" />
              </SelectTrigger>
              <SelectContent>
                {tenants.map((t) => (
                  <SelectItem key={t.slug} value={t.slug}>
                    {t.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Janela de tempo */}
            <Select
              value={String(range)}
              onValueChange={(v) => setRange(Number(v))}
            >
              <SelectTrigger className="hidden sm:flex w-36 h-9 bg-secondary/50 border-border/50 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {RANGE_OPTIONS.map((r) => (
                  <SelectItem key={r} value={String(r)}>
                    {RANGE_LABELS[r]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center gap-2 lg:gap-3">
            <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-md bg-emerald-500/10 border border-emerald-500/20">
              <Bot size={14} className="text-emerald-400" />
              <span className="text-xs font-medium text-emerald-400">
                Agente Ativo
              </span>
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse-dot" />
            </div>

            {/* Mobile: small status dot */}
            <div className="sm:hidden flex items-center gap-1.5 px-2 py-1 rounded-md bg-emerald-500/10">
              <Bot size={12} className="text-emerald-400" />
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse-dot" />
            </div>

            {toggleTheme && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-9 w-9 text-muted-foreground hover:text-foreground"
                    onClick={toggleTheme}
                    aria-label={
                      theme === "dark"
                        ? "Ativar modo claro"
                        : "Ativar modo escuro"
                    }
                  >
                    {theme === "dark" ? (
                      <Sun size={18} />
                    ) : (
                      <Moon size={18} />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="bottom" sideOffset={8}>
                  {theme === "dark" ? "Modo claro" : "Modo escuro"}
                </TooltipContent>
              </Tooltip>
            )}

            <Button
              variant="ghost"
              size="icon"
              className="h-9 w-9 text-muted-foreground hover:text-foreground"
              onClick={() => toast("Notificações em breve")}
            >
              <Bell size={18} />
            </Button>
          </div>
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto p-4 lg:p-6">{children}</main>
      </div>
    </div>
  );
}
