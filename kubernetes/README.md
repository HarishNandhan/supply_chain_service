# Kubernetes Deployment Files

This directory contains all Kubernetes manifests for deploying the Supply Chain Analytics Platform on GKE.

## Files Overview

- `streamlit-deployment.yaml` - Streamlit frontend deployment
- `streamlit-service.yaml` - Streamlit service configuration
- `airflow-values.yaml` - Helm values for Airflow
- `kafka-producer-deployment.yaml` - Kafka producer deployment
- `ingress.yaml` - Ingress configuration for HTTPS
- `hpa.yaml` - Horizontal Pod Autoscaler
- `secrets.yaml` - Secret references (template)

## Quick Deploy

```bash
# Deploy all manifests
kubectl apply -f kubernetes/

# Check status
kubectl get pods -n supply-chain
kubectl get services -n supply-chain
```
