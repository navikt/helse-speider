# Speider
![Bygg og deploy](https://github.com/navikt/helse-speider/workflows/Bygg%20og%20deploy/badge.svg)

## Beskrivelse
Speider etter mikrotjenester som ikke oppfører seg :telescope:.

- Sender ut ping-meldinger og reagerer på pong-meldinger fra apper.
- Lytter på `application_up`- og `application_down`-meldinger og holder oversikt over tilstand på appene
- Sender melding `app_status`, som plukkes opp av [spammer](https://github.com/navikt/helse-spammer)

## Driftsrutiner
- Hvis appen gir falske positiver på Slack, for eksempel fordi en app er tatt ned med vilje: restart appen (`k delete pod speider-...`)
  - Hvis man skal gjøre noe man vet kommer til å få speider til å trigge, kan man også ta ned speider i forkant

## Henvendelser
Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på GitHub.

### For NAV-ansatte
Interne henvendelser kan sendes via Slack i kanalen #område-helse.
