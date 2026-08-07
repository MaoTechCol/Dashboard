from __future__ import annotations

from fastapi import HTTPException, Request, status

from app.core.security import decode_token


def get_context(request: Request):
    return request.app.state.context


def get_current_user(request: Request):
    context = get_context(request)
    token = request.cookies.get(context.settings.session_cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        claims = decode_token(token, secret=context.settings.jwt_secret)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    user = context.auth.get_user(str(claims.get("sub") or ""))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown session user")
    return user


def require_admin(request: Request):
    user = get_current_user(request)
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


def resolve_company_slug(*, request: Request, user, requested_slug: str | None = None) -> str:
    context = get_context(request)
    if user.role == "admin":
        company_slug = requested_slug or user.company_slug
        if not company_slug:
            visible = context.auth.visible_companies(user)
            company_slug = visible[0].slug if visible else None
    else:
        company_slug = user.company_slug
        if requested_slug and requested_slug != company_slug:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You cannot access another company")

    if not company_slug:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No company available for this user")

    company = context.registry.get(company_slug)
    if not context.registry.is_operational(company):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Company is not operationally configured")
    return company.slug
