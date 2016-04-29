package imap_input

import (
	"encoding/json"

	"github.com/veino/veino"
)

type toJsonHandler struct {
	packetFactory veino.PacketBuilder
	send          veino.PacketSender
	packet        veino.IPacket
}

func (hnd *toJsonHandler) Deliver(email string) error {
	docJSON, _ := json.Marshal(getMsg(email))
	e := hnd.packetFactory(string(docJSON), nil)
	hnd.send(e, 0)
	return nil
}

func (hnd *toJsonHandler) Describe() string {
	return "To JSON Handler"
}

func newToJsonHandler(pFactory veino.PacketBuilder, packet veino.IPacket, sender veino.PacketSender) *toJsonHandler {
	return &toJsonHandler{packetFactory: pFactory, packet: packet, send: sender}
}
