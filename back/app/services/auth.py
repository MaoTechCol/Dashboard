from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from app.core.security import create_token, hash_password, verify_password
from app.models import CompanyHistoricalRebuildJob, PublishedDashboardSnapshot, UserAccount
from app.schemas import CompanySummaryView, CompanyConfig, UserSessionView


@dataclass
class AuthResult:
    username: str
    role: str
    company_slug: str | None


class AuthService:
    def __init__(self, *, session_factory: Any, settings: Any, registry: Any) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.registry = registry

    def seed_users(self) -> None:
        with self.session_factory() as session:
            admin = session.scalar(select(UserAccount).where(UserAccount.username == self.settings.seed_admin_username))
            if not admin:
                session.add(
                    UserAccount(
                        username=self.settings.seed_admin_username,
                        password_hash=hash_password(self.settings.seed_admin_password),
                        role="admin",
                        company_slug=None,
                    )
                )
            else:
                admin.role = "admin"
                admin.company_slug = None
                admin.is_active = True
            session.commit()

    def upsert_company_user(
        self,
        *,
        company_slug: str,
        password: str,
        username: str | None = None,
    ) -> AuthResult:
        normalized_username = (username or company_slug).strip().lower()
        normalized_password = (password or "").strip()
        if not normalized_username:
            raise ValueError("No se pudo generar el usuario cliente para la empresa")
        if not normalized_password:
            raise ValueError("Debes asignar una contrasena para crear el usuario cliente")

        with self.session_factory() as session:
            existing_by_username = session.scalar(select(UserAccount).where(UserAccount.username == normalized_username))
            if existing_by_username and existing_by_username.role == "admin":
                raise ValueError(f"El usuario {normalized_username} ya existe como administrador")
            if existing_by_username and existing_by_username.company_slug not in {None, company_slug}:
                raise ValueError(f"El usuario {normalized_username} ya esta asignado a otra empresa")

            company_users = list(session.scalars(select(UserAccount).where(UserAccount.company_slug == company_slug)))
            target = existing_by_username
            for company_user in company_users:
                if company_user.username == normalized_username:
                    target = company_user
                    continue
                session.delete(company_user)

            if target is None:
                session.add(
                    UserAccount(
                        username=normalized_username,
                        password_hash=hash_password(normalized_password),
                        role="client",
                        company_slug=company_slug,
                        is_active=True,
                    )
                )
            else:
                target.password_hash = hash_password(normalized_password)
                target.role = "client"
                target.company_slug = company_slug
                target.is_active = True

            session.commit()

        return AuthResult(username=normalized_username, role="client", company_slug=company_slug)

    def delete_company_users(self, *, company_slug: str) -> int:
        with self.session_factory() as session:
            users = list(session.scalars(select(UserAccount).where(UserAccount.company_slug == company_slug)))
            for user in users:
                session.delete(user)
            session.commit()
            return len(users)

    def change_password(
        self,
        *,
        username: str,
        new_password: str,
        expected_role: str | None = None,
    ) -> AuthResult:
        normalized_username = (username or "").strip().lower()
        normalized_password = (new_password or "").strip()
        if not normalized_username:
            raise ValueError("Debes indicar el usuario que vas a actualizar")
        if not normalized_password:
            raise ValueError("Debes indicar la nueva contrasena")

        with self.session_factory() as session:
            user = session.scalar(select(UserAccount).where(UserAccount.username == normalized_username))
            if not user:
                raise ValueError(f"El usuario {normalized_username} no existe")
            if expected_role and user.role != expected_role:
                raise ValueError(f"El usuario {normalized_username} no corresponde al rol esperado")
            user.password_hash = hash_password(normalized_password)
            user.is_active = True
            session.commit()
            return AuthResult(username=user.username, role=user.role, company_slug=user.company_slug)

    def authenticate(self, username: str, password: str) -> AuthResult | None:
        with self.session_factory() as session:
            user = session.scalar(select(UserAccount).where(UserAccount.username == username, UserAccount.is_active.is_(True)))
            if not user or not verify_password(password, user.password_hash):
                return None
            return AuthResult(username=user.username, role=user.role, company_slug=user.company_slug)

    def create_session_token(self, user: AuthResult) -> str:
        return create_token(
            {
                "sub": user.username,
                "role": user.role,
                "company_slug": user.company_slug,
            },
            secret=self.settings.jwt_secret,
            ttl_minutes=self.settings.session_ttl_minutes,
        )

    def get_user(self, username: str) -> AuthResult | None:
        with self.session_factory() as session:
            user = session.scalar(select(UserAccount).where(UserAccount.username == username, UserAccount.is_active.is_(True)))
            if not user:
                return None
            return AuthResult(username=user.username, role=user.role, company_slug=user.company_slug)

    def serialize_session(self, user: AuthResult) -> UserSessionView:
        company_name = None
        if user.company_slug:
            try:
                company_name = self.registry.get(user.company_slug).name
            except KeyError:
                company_name = None
        return UserSessionView(
            username=user.username,
            role="admin" if user.role == "admin" else "client",
            company_slug=user.company_slug,
            company_name=company_name,
        )

    def visible_companies(self, user: AuthResult) -> list[CompanyConfig]:
        self.registry.reload()
        with self.session_factory() as session:
            if user.role == "admin":
                return [
                    company
                    for company in self.registry.all()
                    if self.registry.is_operational(company) and self._company_ready_for_portal(session, company.slug)
                ]
            if not user.company_slug:
                return []
            try:
                company = self.registry.get(user.company_slug)
            except KeyError:
                return []
            if not self.registry.is_operational(company):
                return []
            return [company] if self._company_ready_for_portal(session, company.slug) else []

    def serialize_companies(self, companies: list[CompanyConfig]) -> list[CompanySummaryView]:
        return [
            CompanySummaryView(
                slug=company.slug,
                name=company.name,
                customer=company.customer,
                timezone=company.timezone,
                brand=company.brand,
            )
            for company in companies
        ]

    def _company_ready_for_portal(self, session: Any, company_slug: str) -> bool:
        latest_rebuild = session.scalar(
            select(CompanyHistoricalRebuildJob)
            .where(
                CompanyHistoricalRebuildJob.company_slug == company_slug,
                CompanyHistoricalRebuildJob.purpose == "activation_bootstrap",
            )
            .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
        )
        if latest_rebuild and latest_rebuild.status in {"queued", "running", "failed"}:
            return False

        publication = session.get(PublishedDashboardSnapshot, company_slug)
        if not publication:
            return False
        return bool(publication.snapshot_json)
