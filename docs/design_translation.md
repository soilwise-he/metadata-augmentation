
# Design Document: Translation

## Introduction

Some imported records are in a non-english language. In order to imrpove their discoverability the component uses EU translation service to translate the title and abstract to english. The keywords are not translated, because they are translated through the keyword matcher.

### Component Overview and Scope

### Users

### References

## Requirements

### Functional Requirements

- Identify if the record benefits from translation
- Identify the source language
- Request the translation
- Receive the translation (asynchronous)
- Update the catalogue with translated content

### Non-functional Requirements

- Prevent that the service is mis-used as a proxy to the EU translate service

## Architecture

### Technological Stack

### Overview of Key Features

### Component Diagrams

### Sequence Diagram

### Database Design

### Integrations & Interfaces

### Key Architectural Decisions

## Risks & Limitations