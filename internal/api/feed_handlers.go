package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/landing"
)

func (s *Server) startIsolation(writer http.ResponseWriter, request *http.Request) {
	stationID := chi.URLParam(request, "id")
	var input IsolationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	permit, err := s.services.Permits.Request(stationID, input.CircuitID)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("feed-isolation", permit.ID, permit); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, permit)
}

func (s *Server) recordReceipt(writer http.ResponseWriter, request *http.Request) {
	stationID := chi.URLParam(request, "id")
	var input ReceiptRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	receipt := landing.IsolationReceipt{
		StationID: stationID, CircuitID: input.CircuitID, OperationID: input.OperationID,
		Isolated: input.Isolated, ReceivedAt: s.services.Clock.Now().UTC(),
	}
	// Reserve first only to claim the submission slot; the reservation stays
	// invisible to permit evaluation until persistence succeeds.
	existing, committed, err := s.services.Receipts.Reserve(receipt)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if committed {
		// Idempotent replay of an already-committed receipt: do not append
		// another journal record and return the first-committed payload.
		writeJSON(writer, http.StatusCreated, existing)
		return
	}
	if err := s.services.Persist("feed-receipt", input.OperationID, receipt); err != nil {
		// Compensate: drop the reservation so the failed response leaves the
		// registry unchanged and the client can retry.
		s.services.Receipts.Rollback(receipt)
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	// Persisted first, only now make the receipt visible to Match.
	s.services.Receipts.Publish(receipt.StationID, receipt.CircuitID, receipt.OperationID)
	writeJSON(writer, http.StatusCreated, receipt)
}

func (s *Server) listReceipts(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Receipts.ForStation(chi.URLParam(request, "id"))})
}

func (s *Server) evaluatePermit(writer http.ResponseWriter, request *http.Request) {
	permit, err := s.services.Permits.Evaluate(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("permit-groundable", permit.ID, permit); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}

func (s *Server) getPermit(writer http.ResponseWriter, request *http.Request) {
	permit, exists := s.services.Permits.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "permit not found"})
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}

func (s *Server) groundPermit(writer http.ResponseWriter, request *http.Request) {
	permit, err := s.services.Permits.Ground(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}
