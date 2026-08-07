from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from app.core.security import create_token, hash_password, verify_password
from app.models import UserAccount
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

            for company in self.registry.all():
                client = session.scalar(select(UserAccount).where(UserAccount.username == company.slug))
                if not client:
                    session.add(
                        UserAccount(
                            username=company.slug,
                            password_hash=hash_password(self.settings.seed_client_password),
                            role="client",
                            company_slug=company.slug,
                        )
                    )
            session.commit()

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
        if user.role == "admin":
            return [company for company in self.registry.all() if self.registry.is_operational(company)]
        if not user.company_slug:
            return []
        try:
            company = self.registry.get(user.company_slug)
        except KeyError:
            return []
        return [company] if self.registry.is_operational(company) else []

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
